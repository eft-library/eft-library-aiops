"""Deterministic operator-label rollups, pinned to selected daily versions."""
import hashlib
import json
from collections import Counter

from aiops.providers.progress import PublicProgress
from aiops.reporting.store import encoded


def evaluation_rollup(settings, reports):
    from aiops.storage.evaluations import finding_targets
    from aiops.storage.postgres import connect

    targets, labels = [], []
    report_ids = [row['report_id'] for row in reports]
    if settings.database_url and report_ids:
        with connect(settings) as db, db.transaction():
            db.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY')
            for report_id in report_ids:
                targets.extend({'report_id': report_id, **target} for target in finding_targets(db, report_id))
            labels = db.execute('''SELECT DISTINCT ON (report_id, finding_key) *
                FROM public.aiops_finding_evaluations WHERE report_id = ANY(%s)
                ORDER BY report_id, finding_key, revision DESC''', (report_ids,)).fetchall()
    elif not settings.database_url:
        for row in reports:
            result = json.loads(row['result_json'])['report']
            for section in ('findings', 'incident_analysis', 'performance_analysis'):
                findings = result.get('findings', []) if section == 'findings' else (result.get(section) or {}).get('findings', [])
                targets.extend({'report_id': row['report_id'], 'finding_key': f'report:{section}:{index}',
                                'finding': finding} for index, finding in enumerate(findings))
    eligible = {(target['report_id'], target['finding_key']) for target in targets}
    labels = [label for label in labels if (label['report_id'], label['finding_key']) in eligible]
    verdicts = Counter(label['verdict'] for label in labels)
    usefulness = Counter(label['usefulness'] for label in labels if label['usefulness'])
    cleaner = PublicProgress(settings, lambda event: None)

    def clean(value):
        if isinstance(value, str):
            return cleaner.clean(value)
        if isinstance(value, dict):
            return {key: clean(item) for key, item in value.items()}
        if isinstance(value, list):
            return [clean(item) for item in value]
        return value

    items = [clean({key: label[key] for key in (
        'evaluation_id', 'report_id', 'finding_key', 'revision', 'verdict',
        'actual_result', 'evidence', 'usefulness', 'finding_snapshot')}) for label in labels]
    for item, label in zip(items, labels):
        item['evaluated_at'] = label['created_at'].isoformat()
    items.sort(key=lambda item: (item['report_id'], item['finding_key']))
    signature = hashlib.sha256(encoded({
        'targets': sorted(eligible),
        'revisions': [(item['evaluation_id'], item['revision']) for item in items],
    }).encode()).hexdigest()
    total, evaluated = len(targets), len(labels)
    return {
        'schema_version': 1, 'signature': signature,
        'status': 'unavailable' if not settings.database_url else ('observed' if evaluated else 'no_evaluations'),
        'source_report_ids': report_ids,
        'total_findings': total, 'evaluated_findings': evaluated, 'unevaluated_findings': total - evaluated,
        'evaluation_coverage_percent': round(100 * evaluated / total, 2) if total else None,
        'verdict_counts': {key: verdicts[key] for key in ('correct', 'partial', 'incorrect', 'unconfirmed')},
        'usefulness_counts': {key: usefulness[key] for key in ('helpful', 'neutral', 'unhelpful')},
        'confirmed_findings': evaluated - verdicts['unconfirmed'],
        'items': items, 'omitted_details': 0,
        'limitations': [
            'Counts describe finding evaluations, not unique incidents. Sections and investigations may overlap.',
            'Only latest labels on selected daily report versions and their latest ten completed investigations are counted.',
            'Weekly context is excluded from these counts. Unevaluated and unconfirmed are not incorrect.',
            'Operator labels are reported confirmations, not independently verified root causes or representative accuracy.',
            'No statistical trend or accuracy score is established; always report sample size and coverage.',
            'This is a preparation-time snapshot; later label edits do not alter this report version.',
        ],
    }
