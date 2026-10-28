import psycopg

from aiops.cli import main
from aiops.config import Settings


def test_database_credentials_are_not_in_config_repr(monkeypatch):
    monkeypatch.setenv(
        "AIOPS_DATABASE_URL", "postgresql://user:do-not-print@example.invalid/db"
    )
    assert "do-not-print" not in repr(Settings.from_env())


def test_connection_failure_does_not_print_dsn_or_server_message(monkeypatch, capsys):
    def fail(*args, **kwargs):
        raise psycopg.OperationalError("do-not-print-sensitive-server-data")

    monkeypatch.setenv(
        "AIOPS_DATABASE_URL", "postgresql://user:private-password@example.invalid/db"
    )
    monkeypatch.setattr(psycopg, "connect", fail)
    assert main(["db", "doctor"]) == 11
    output = capsys.readouterr()
    assert output.out == ""
    assert "private-password" not in output.err and "sensitive" not in output.err
    assert "database" in output.err


def test_print_sql_never_connects_or_exposes_environment(monkeypatch, capsys):
    monkeypatch.setenv(
        "AIOPS_DATABASE_URL", "postgresql://user:private-password@example.invalid/db"
    )
    monkeypatch.setattr(
        psycopg,
        "connect",
        lambda *a, **kw: (_ for _ in ()).throw(AssertionError("no DB access")),
    )
    assert main(["db", "print-sql"]) == 0
    output = capsys.readouterr()
    assert "CREATE TABLE" in output.out and "private-password" not in output.out
