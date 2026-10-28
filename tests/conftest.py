import pytest


@pytest.fixture(autouse=True)
def isolate_database_environment(monkeypatch):
    # A developer's exported production DSN must never opt regular tests into DB writes.
    # PostgreSQL tests explicitly construct their own ephemeral-cluster settings.
    monkeypatch.delenv('AIOPS_DATABASE_URL', raising=False)
