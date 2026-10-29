"""Bounded, read-only queries for the reporting website."""

from aiops.storage.jobs import public_job
from aiops.storage.postgres import connect, require_schema


def connection(settings):
    db = connect(settings)
    try:
        require_schema(db)
        db.execute("SET default_transaction_read_only = on")
    except BaseException:
        db.close()
        raise
    return db


def report_summary(row):
    row = public_job(row)
    row["period"] = {
        "start": row.pop("period_start"), "end": row.pop("period_end"),
        "timezone": "Asia/Seoul", "end_exclusive": True,
    }
    return row


def reports(settings, *, kind=None, status=None, severity=None, start=None, end=None,
            page=1, page_size=12, history=False):
    clauses, params = [], []
    for column, value in (("kind", kind), ("status", status), ("severity", severity)):
        if value is not None:
            clauses.append(f"{column} = %s")
            params.append(value)
    if start:
        clauses.append("period_start >= %s")
        params.append(start)
    if end:
        clauses.append("period_start <= %s")
        params.append(end)
    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    latest = "" if history else " WHERE version_rank=1"
    base = f"""WITH ranked AS (
        SELECT v.report_id,v.period_id,v.revision,v.status,v.created_at,v.completed_at,
               v.coverage_complete,v.error,p.kind,p.period_start,p.period_end,
               v.result_json->'report'->>'severity' AS severity,
               v.result_json->'report'->>'summary' AS summary,
               v.result_json->>'model' AS model,
               v.input_json->'data_quality' AS data_quality,
               COALESCE(v.input_json->'totals',v.input_json->'observed_totals') AS totals,
               row_number() OVER(PARTITION BY v.period_id ORDER BY v.revision DESC) AS version_rank
        FROM public.aiops_report_versions v JOIN public.aiops_reports p USING(period_id)
    ), selected AS (SELECT * FROM ranked{latest})"""
    with connection(settings) as db:
        total = db.execute(base + " SELECT count(*) AS total FROM selected" + where, params).fetchone()["total"]
        rows = db.execute(
            base + " SELECT * FROM selected" + where
            + " ORDER BY period_start DESC,kind,revision DESC LIMIT %s OFFSET %s",
            [*params, page_size, (page - 1) * page_size],
        ).fetchall()
    items = []
    for row in rows:
        row.pop("version_rank")
        row.pop("period_id")
        items.append(report_summary(row))
    return {"items": items, "total": total, "page": page, "page_size": page_size}


def report_detail(settings, report_id):
    with connection(settings) as db:
        row = db.execute("""SELECT v.*,p.kind,p.period_start,p.period_end
            FROM public.aiops_report_versions v JOIN public.aiops_reports p USING(period_id)
            WHERE report_id=%s""", (report_id,)).fetchone()
    if row is None:
        return None
    from aiops.investigation.store import for_report

    with connection(settings) as db:
        investigations = for_report(db, report_id) if row["kind"] == "daily" else []
    source = row.pop("input_json")
    manifest = row.pop("manifest_json")
    result = row.pop("result_json")
    row.pop("period_id")
    row.pop("fingerprint")
    evidence = {}
    for event in source.get("events", []):
        evidence[event["event_id"]] = event
    for group in source.get("error_groups", []):
        for evidence_id in group.get("evidence_ids", []):
            evidence.setdefault(evidence_id, {"event_id": evidence_id, **group})
    for metric in source.get("metrics", []):
        for evidence_id in metric.get("evidence_ids", []):
            evidence[evidence_id] = metric
    for child in [*source.get("daily_reports", []), *source.get("weekly_context", [])]:
        evidence[child["report_id"]] = child
    return {
        **report_summary(row), "result": result, "investigations": investigations,
        "data_quality": source.get("data_quality", {}),
        "totals": source.get("totals", source.get("observed_totals", {})),
        "source_batch_count": source.get("source_batch_count"),
        "input_limitations": source.get("limitations", []),
        "evidence": evidence,
        "lineage": {key: manifest[key] for key in (
            "batch_ids", "daily_report_ids", "missing_dates", "outdated_report_ids", "weekly_context_ids", "investigation_ids"
        ) if key in manifest},
    }


def collections(settings, *, page=1, page_size=12):
    with connection(settings) as db:
        total = db.execute("SELECT count(*) AS total FROM public.aiops_batches").fetchone()["total"]
        rows = db.execute("""SELECT batch_id,collected_at,period_start,period_end,coverage,totals
            FROM public.aiops_batches ORDER BY collected_at DESC,batch_id
            LIMIT %s OFFSET %s""", (page_size, (page - 1) * page_size)).fetchall()
    return {"items": [public_job(row) for row in rows], "total": total,
            "page": page, "page_size": page_size}


def jobs(settings, *, status=None, page=1, page_size=12):
    where = " WHERE status=%s" if status else ""
    params = [status] if status else []
    with connection(settings) as db:
        total = db.execute("SELECT count(*) AS total FROM public.aiops_analysis_jobs" + where, params).fetchone()["total"]
        rows = db.execute("SELECT * FROM public.aiops_analysis_jobs" + where
            + " ORDER BY requested_at DESC,job_id LIMIT %s OFFSET %s",
            [*params, page_size, (page - 1) * page_size]).fetchall()
    return {"items": [public_job(row) for row in rows], "total": total,
            "page": page, "page_size": page_size}
