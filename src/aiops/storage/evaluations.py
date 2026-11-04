"""Operator labels are append-only and separate from AI-produced evidence."""
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field
from psycopg.types.json import Jsonb

from aiops.storage.postgres import connect


class EvaluationInput(BaseModel):
    model_config = ConfigDict(extra='forbid')
    finding_key: str = Field(min_length=1, max_length=200)
    expected_revision: int = Field(ge=0)
    verdict: Literal['correct', 'partial', 'incorrect', 'unconfirmed']
    actual_result: str = Field(default='', max_length=4000)
    evidence: str = Field(default='', max_length=4000)
    usefulness: Literal['helpful', 'neutral', 'unhelpful'] | None = None
    reviewer: str = Field(min_length=1, max_length=128)


def finding_targets(db, report_id, *, lock=False):
    row = db.execute('SELECT status, result_json FROM public.aiops_report_versions WHERE report_id=%s' +
                     (' FOR UPDATE' if lock else ''), (report_id,)).fetchone()
    if row is None or row['status'] != 'COMPLETED':
        raise LookupError('completed_report_required')
    report = (row['result_json'] or {}).get('report', {})
    targets = []

    def add(prefix, title, findings):
        for index, finding in enumerate(findings or []):
            targets.append({'finding_key': f'{prefix}:{index}', 'section': title,
                            'finding': finding})

    add('report:findings', '주요 발견 사항', report.get('findings'))
    for section, title in [('incident_analysis', '장애 분석'), ('performance_analysis', '성능 분석')]:
        add('report:' + section, title, (report.get(section) or {}).get('findings'))
    for investigation in db.execute('''SELECT investigation_id, result_json FROM public.aiops_investigations
            WHERE report_id=%s AND status='COMPLETED' ORDER BY finished_at DESC LIMIT 10''', (report_id,)):
        add('investigation:' + str(investigation['investigation_id']), '심화 조사',
            (investigation['result_json'] or {}).get('report', {}).get('findings'))
    return targets


def public_row(row):
    result = dict(row)
    result['created_at'] = result['created_at'].isoformat()
    return result


def read_evaluations(settings, report_id):
    with connect(settings) as db:
        targets = finding_targets(db, report_id)
        for target in targets:
            rows = db.execute('''SELECT * FROM public.aiops_finding_evaluations
                WHERE report_id=%s AND finding_key=%s ORDER BY revision DESC LIMIT 20''',
                (report_id, target['finding_key'])).fetchall()
            target['history'] = [public_row(row) for row in rows]
            target['latest'] = target['history'][0] if rows else None
    return {'items': targets, 'history_limit': 20,
            'scope': 'Report-version-specific operator evaluations; unconfirmed is not incorrect.'}


def save_evaluation(settings, report_id, body):
    with connect(settings) as db, db.transaction():
        targets = finding_targets(db, report_id, lock=True)
        target = next((target for target in targets if target['finding_key'] == body.finding_key), None)
        if target is None:
            raise LookupError('unknown_finding')
        latest = db.execute('''SELECT revision FROM public.aiops_finding_evaluations
            WHERE report_id=%s AND finding_key=%s ORDER BY revision DESC LIMIT 1''',
            (report_id, body.finding_key)).fetchone()
        revision = latest['revision'] if latest else 0
        if revision != body.expected_revision:
            raise FileExistsError('evaluation_conflict')
        row = db.execute('''INSERT INTO public.aiops_finding_evaluations
            (report_id, finding_key, revision, finding_snapshot, verdict, actual_result, evidence, usefulness, reviewer)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
            (report_id, body.finding_key, revision + 1, Jsonb(target['finding']), body.verdict,
             body.actual_result, body.evidence, body.usefulness, body.reviewer)).fetchone()
        return public_row(row)
