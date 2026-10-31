"""Durable follow-up queue. Daily report state is never mutated."""

import hashlib
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from aiops.investigation.models import INSTRUCTIONS, PLANNING_INSTRUCTIONS
from aiops.storage.jobs import RETRYABLE, public_job
from aiops.storage.postgres import connect, require_schema

POLICY_HASH = hashlib.sha256((INSTRUCTIONS + PLANNING_INSTRUCTIONS + "typed-probes-v2").encode()).hexdigest()


class InvalidParent(ValueError):
    pass


def available(db):
    return bool(db.execute("SELECT to_regclass('public.aiops_investigations') AS name").fetchone()['name'])


def public(row, *, detail=False):
    item = public_job(row)
    item['job_id'] = item['investigation_id']
    item.pop('worker_id', None)
    item.pop('policy_hash', None)
    if detail:
        item['input'] = item.pop('input_json', None)
        item['result'] = item.pop('result_json', None)
    else:
        item.pop('input_json', None)
        item.pop('result_json', None)
    return item


class InvestigationStore:
    def __init__(self, settings):
        self.db = connect(settings)
        try:
            require_schema(self.db)
            if not available(self.db):
                raise ValueError('database migration 3 is required')
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def enqueue(self, report_id, request_key, model):
        # Keep caller retries idempotent while preserving results from earlier policies.
        request_key = hashlib.sha256((request_key + "\0" + POLICY_HASH).encode()).hexdigest()
        with self.db.transaction():
            parent = self.db.execute("""SELECT v.status,p.kind FROM public.aiops_report_versions v
                JOIN public.aiops_reports p USING(period_id) WHERE report_id=%s FOR SHARE OF v""", (report_id,)).fetchone()
            if not parent or parent['kind'] != 'daily' or parent['status'] != 'COMPLETED':
                raise InvalidParent('completed daily report required')
            self.db.execute("""INSERT INTO public.aiops_investigations
                (investigation_id,report_id,request_key,status,model,policy_hash)
                VALUES (%s,%s,%s,'PENDING',%s,%s) ON CONFLICT(report_id,request_key) DO NOTHING""",
                (uuid4(), report_id, request_key, model, POLICY_HASH))
            return public(self.db.execute("SELECT * FROM public.aiops_investigations WHERE report_id=%s AND request_key=%s",
                                          (report_id, request_key)).fetchone())

    def get(self, investigation_id):
        row = self.db.execute('SELECT * FROM public.aiops_investigations WHERE investigation_id=%s',
                              (UUID(str(investigation_id)),)).fetchone()
        return public(row, detail=True) if row else None

    def parent(self, report_id):
        return self.db.execute("""SELECT v.*,p.kind,p.period_start,p.period_end FROM public.aiops_report_versions v
            JOIN public.aiops_reports p USING(period_id) WHERE report_id=%s""", (report_id,)).fetchone()

    def claim(self, worker_id, investigation_id=None):
        where = ' AND investigation_id=%s' if investigation_id else ''
        params = (UUID(str(investigation_id)),) if investigation_id else ()
        with self.db.transaction():
            row = self.db.execute("""SELECT * FROM public.aiops_investigations WHERE status='PENDING'
                AND available_at<=now()""" + where + ' ORDER BY requested_at,investigation_id FOR UPDATE SKIP LOCKED LIMIT 1', params).fetchone()
            if not row:
                return None
            row = self.db.execute("""UPDATE public.aiops_investigations SET status='RUNNING',worker_id=%s,
                attempt_count=attempt_count+1,started_at=now(),finished_at=NULL,error=NULL
                WHERE investigation_id=%s RETURNING *""", (worker_id, row['investigation_id'])).fetchone()
            self.db.execute("""INSERT INTO public.aiops_investigation_attempts(investigation_id,attempt)
                VALUES (%s,%s)""", (row['investigation_id'], row['attempt_count']))
            return row

    def save_input(self, job, worker_id, snapshot):
        with self.db.transaction():
            row = self.db.execute("""UPDATE public.aiops_investigations SET input_json=%s
                WHERE investigation_id=%s AND worker_id=%s AND status='RUNNING' AND attempt_count=%s RETURNING investigation_id""",
                (Jsonb(snapshot), job['investigation_id'], worker_id, job['attempt_count'])).fetchone()
            if not row:
                raise ValueError('lease lost')
            self.db.execute("""UPDATE public.aiops_investigation_attempts SET input_json=%s
                WHERE investigation_id=%s AND attempt=%s""", (Jsonb(snapshot), job['investigation_id'], job['attempt_count']))

    def finish(self, job, worker_id, *, result=None, error=None):
        with self.db.transaction():
            row = self.db.execute('SELECT * FROM public.aiops_investigations WHERE investigation_id=%s FOR UPDATE',
                                  (job['investigation_id'],)).fetchone()
            if row['worker_id'] != worker_id or row['status'] != 'RUNNING' or row['attempt_count'] != job['attempt_count']:
                return None
            retry = error == 'busy' or (error in RETRYABLE and row['attempt_count'] < row['max_attempts'])
            state = 'COMPLETED' if result is not None else 'PENDING' if retry else 'TIMEOUT' if error == 'timeout' else 'FAILED'
            self.db.execute("""UPDATE public.aiops_investigation_attempts SET finished_at=now(),error=%s,result_json=%s
                WHERE investigation_id=%s AND attempt=%s""", (error, Jsonb(result), row['investigation_id'], row['attempt_count']))
            row = self.db.execute("""UPDATE public.aiops_investigations SET status=%s,error=%s,result_json=%s,
                available_at=now()+(%s * interval '1 second'),worker_id=NULL,
                finished_at=CASE WHEN %s='PENDING' THEN NULL ELSE now() END
                WHERE investigation_id=%s RETURNING *""",
                (state, error, Jsonb(result), 5 if error == 'busy' else min(3600, 30 * 2 ** (row['attempt_count']-1)) if retry else 0,
                 state, row['investigation_id'])).fetchone()
            if error == 'busy':
                self.db.execute('DELETE FROM public.aiops_investigation_attempts WHERE investigation_id=%s AND attempt=%s',
                                (row['investigation_id'], row['attempt_count']))
                row = self.db.execute('UPDATE public.aiops_investigations SET attempt_count=attempt_count-1 WHERE investigation_id=%s RETURNING *',
                                      (row['investigation_id'],)).fetchone()
            return public(row)

    def recover(self):
        for row in self.db.execute("SELECT * FROM public.aiops_investigations WHERE status='RUNNING'").fetchall():
            self.finish(row, row['worker_id'], error='interrupted')


def for_report(db, report_id):
    if not available(db):
        return []
    rows = db.execute("""SELECT * FROM public.aiops_investigations WHERE report_id=%s
        ORDER BY requested_at DESC,investigation_id DESC LIMIT 5""", (report_id,)).fetchall()
    return [public(row, detail=True) for row in rows]


def completed_summaries(settings, report_ids):
    if not settings.database_url or not report_ids:
        return {}
    with connect(settings) as db:
        db.execute('SET default_transaction_read_only = on')
        if not available(db):
            return {}
        rows = db.execute("""SELECT DISTINCT ON (report_id) * FROM public.aiops_investigations
            WHERE report_id=ANY(%s) AND status='COMPLETED'
            ORDER BY report_id,requested_at DESC,investigation_id DESC""", (list(report_ids),)).fetchall()
    return {row['report_id']: {
        'investigation_id': str(row['investigation_id']), 'finished_at': row['finished_at'].isoformat(),
        'summary': row['result_json']['report']['summary'][:1000],
        'findings': [{'issue': finding['issue'][:300], 'assessment': finding['assessment'][:400],
                      'proposed_responses': [r['action'][:300] for r in finding['responses'][:2]]}
                     for finding in row['result_json']['report']['findings'][:3]],
        'limitations': [text[:200] for text in row['result_json']['report']['limitations'][:3]],
        'scope': 'Follow-up observations after the daily period; proposed responses, not performed actions.'
    } for row in rows}
