"""Fixed read-only service DB query. response_ms stores SECONDS in the Airflow producer."""

import math
from dataclasses import replace
from datetime import timedelta

import psycopg

from aiops.providers.base import ProviderError
from aiops.storage.postgres import connect

SQL = """WITH source AS (
 SELECT service_name,checked_time,response_ms,
   response_ms IS NOT NULL AND response_ms::text NOT IN ('NaN','Infinity','-Infinity')
     AND response_ms>=0 AS valid,
   date_part('hour',checked_time AT TIME ZONE 'Asia/Seoul')::integer AS hour_kst,
   CASE WHEN checked_time >= %s THEN 'current' ELSE 'previous' END AS period
 FROM public.response_time WHERE checked_time >= %s AND checked_time < %s
 AND service_name IN ('FastAPI','Next.js')
)
SELECT service_name,period,hour_kst,count(*) AS measurements,
 count(*) FILTER(WHERE response_ms IS NULL) AS failed_measurements,
 count(*) FILTER(WHERE NOT valid AND response_ms IS NOT NULL) AS invalid_measurements,
 count(*) FILTER(WHERE valid) AS samples,
 count(DISTINCT floor(extract(epoch FROM checked_time)/300)) AS observed_slots,
 min(response_ms*1000) FILTER(WHERE valid) AS min_ms,
 avg(response_ms*1000) FILTER(WHERE valid) AS mean_ms,
 max(response_ms*1000) FILTER(WHERE valid) AS max_ms,
 percentile_cont(0.95) WITHIN GROUP(ORDER BY (response_ms*1000)::double precision) FILTER(WHERE valid) AS p95_ms,
 percentile_cont(0.99) WITHIN GROUP(ORDER BY (response_ms*1000)::double precision) FILTER(WHERE valid) AS p99_ms,
 min(checked_time) AS first_at,max(checked_time) AS last_at
 FROM source GROUP BY GROUPING SETS ((service_name,period),(service_name,period,hour_kst))
 ORDER BY service_name,period,hour_kst NULLS FIRST
"""


def scheduled_health_metrics(settings, period):
    if not settings.metrics_database_url:
        return {'status':'not_configured','services':[],
                'limitations':['Service health response measurements are not configured; no measurements were queried.']}
    try:
        with connect(replace(settings,database_url=settings.metrics_database_url)) as db:
            with db.transaction():
                db.execute('SET TRANSACTION READ ONLY')
                db.execute("SET LOCAL statement_timeout = '3000ms'")
                kind=db.execute("""SELECT udt_name FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='response_time' AND column_name='checked_time'""").fetchone()
                if not kind or kind['udt_name']!='timestamptz':
                    raise ValueError('timezone-aware timestamp required')
                rows=db.execute(SQL,(period.start,period.start-timedelta(days=1),period.end)).fetchall()
        services=[]
        for name in ('FastAPI','Next.js'):
            def values(which):
                row=next((r for r in rows if r['service_name']==name and r['period']==which and r['hour_kst'] is None),None)
                result={'measurements':0,'samples':0,'failed_measurements':0,'invalid_measurements':0,'observed_slots':0}
                if row:
                    for key,value in row.items():
                        if key in {'service_name','period','hour_kst'}:continue
                        if key.endswith('_ms'):
                            value=float(value) if value is not None else None
                            if value is not None and not math.isfinite(value):value=None
                        elif key.endswith('_at'):value=value.isoformat() if value else None
                        result[key]=value
                result['expected_slots']=288
                result['missing_slots']=max(0,288-result['observed_slots'])
                result['duplicate_slot_measurements']=max(0,result['measurements']-result['observed_slots'])
                return result
            current,previous=values('current'),values('previous')
            services.append({'service':name,'evidence_ids':[f'health-probe-{name.lower().replace(".","")}-{period.label}'],
                             'current':current,'previous_day':previous,
                             'hourly_kst':[{'hour':row['hour_kst'], 'samples':row['samples'],
                                            'mean_ms':float(row['mean_ms']) if row['mean_ms'] is not None else None,
                                            'max_ms':float(row['max_ms']) if row['max_ms'] is not None else None,
                                            'failed_measurements':row['failed_measurements']}
                                           for row in rows if row['service_name']==name and row['period']=='current' and row['hour_kst'] is not None]})
        return {'status':'observed','scope':'Airflow five-minute HTTPS /health probes, not all user traffic. Network/proxy/server/response transfer contribute to elapsed time. HTTP status was not persisted; HTTP errors may have a duration.',
                'source_unit':'seconds','display_unit':'ms','timezone':'Asia/Seoul',
                'period':period.as_dict(),'services':services,
                'limitations':['Probe p95/p99 describe sparse scheduled samples only, not user request percentiles.',
                               'NULL is a failed measurement; missing rows are unknown, not successful checks.',
                               'Prior-day comparison requires compatible measurement and timestamp conventions.']}
    except (psycopg.Error,ProviderError,OSError,ValueError):
        return {'status':'unavailable','services':[],
                'limitations':['Service response measurements unavailable: verify read access, table/column types, connectivity and query budget. No source values or credentials are exposed.']}


def attach_health_metrics(snapshot,manifest,settings,period):
    if not settings.metrics_database_url:
        return
    summary=scheduled_health_metrics(settings,period)
    snapshot['scheduled_health_probes']=summary
    manifest['scheduled_health_probes']=summary
    snapshot['limitations'].extend(summary['limitations'])
    for service in summary['services']:
        current=service['current']
        if current['samples']:
            snapshot['metrics'].append({'name':'scheduled_health_response_time','service':service['service'],
                'target':service['service']+' /health periodic probe','unit':'ms','samples':current['samples'],
                'min':current['min_ms'],'max':current['max_ms'],'mean':current['mean_ms'],
                'p95':current['p95_ms'],'p99':current['p99_ms'],'evidence_ids':service['evidence_ids'],
                'scope':'Scheduled synthetic probe; not all HTTP requests.'})


def health_brief(value):
    if not value:
        return None
    return {'status': value['status'], 'unit': 'ms', 'scope': 'Five-minute health probes, not all requests.',
            'services': [{'service': service['service'],
                'current': {key: service['current'].get(key) for key in (
                    'samples', 'mean_ms', 'max_ms', 'p95_ms', 'p99_ms', 'failed_measurements', 'missing_slots')}}
                for service in value.get('services', [])]}
