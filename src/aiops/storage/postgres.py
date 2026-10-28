"""Connections and explicit migrations. Never include DSNs or server errors in output."""

from contextlib import contextmanager
from importlib.resources import files

import psycopg
from psycopg.rows import dict_row

from aiops.providers.base import ErrorKind, ProviderError

WORKER_LOCK = 298735870795
CODEX_LOCK = 298735870796
CATALOGUE_LOCK = 298735870797


def connect(settings):
    if not settings.database_url:
        raise ValueError("AIOPS_DATABASE_URL is required")
    try:
        return psycopg.connect(
            settings.database_url,
            autocommit=True,
            row_factory=dict_row,
            connect_timeout=5,
            application_name="eft-library-aiops",
            options="-c statement_timeout=10000 -c lock_timeout=3000",
        )
    except (psycopg.Error, ValueError):
        raise ProviderError(ErrorKind.DATABASE) from None


def require_schema(conn):
    row = conn.execute(
        "SELECT version FROM aiops.schema_migrations WHERE version=1"
    ).fetchone()
    if not row:
        raise ValueError("database migration 1 is required")


@contextmanager
def advisory_lock(settings, key):
    conn = connect(settings)
    try:
        if not conn.execute(
            "SELECT pg_try_advisory_lock(%s) AS acquired", (key,)
        ).fetchone()["acquired"]:
            raise ProviderError(ErrorKind.BUSY)
        yield conn
    finally:
        conn.close()


def migration_sql():
    return files("aiops.storage").joinpath("sql/001_initial.sql").read_text()


def initialize(settings):
    with connect(settings) as conn:
        conn.execute(migration_sql(), prepare=False)
    return {"status": "ready", "schema": "aiops", "migration": 1}


def doctor(settings):
    with connect(settings) as conn:
        require_schema(conn)
        row = conn.execute(
            "SELECT current_setting('server_version') AS version"
        ).fetchone()
        return {
            "status": "ready",
            "schema": "aiops",
            "migration": 1,
            "server_version": row["version"],
        }
