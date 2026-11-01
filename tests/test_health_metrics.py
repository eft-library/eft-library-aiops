from dataclasses import replace
from datetime import datetime
from zoneinfo import ZoneInfo

from aiops.config import Settings
from aiops.reporting.health_metrics import scheduled_health_metrics, attach_health_metrics
from aiops.reporting.periods import for_period


def period():
    return for_period('daily','2026-11-01',now=datetime(2026,11,2,tzinfo=ZoneInfo('Asia/Seoul')))


def test_optional_connection_and_no_secret_in_settings_repr():
    assert scheduled_health_metrics(Settings(),period())['status']=='not_configured'
    settings=replace(Settings(),metrics_database_url='private-secret')
    assert 'private-secret' not in repr(settings)


def test_failure_is_isolated_without_exposing_dsn(monkeypatch):
    def fail(*args):raise ValueError('private-secret')
    monkeypatch.setattr('aiops.reporting.health_metrics.connect',fail)
    result=scheduled_health_metrics(replace(Settings(),metrics_database_url='private-secret'),period())
    assert result['status']=='unavailable'
    assert 'private-secret' not in str(result)
    snapshot={'metrics':[],'limitations':[]};manifest={}
    attach_health_metrics(snapshot,manifest,replace(Settings(),metrics_database_url='private-secret'),period())
    assert snapshot['scheduled_health_probes']['status']=='unavailable'
