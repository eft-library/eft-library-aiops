import pytest
from fastapi.testclient import TestClient

from aiops.api import create_app
from aiops.config import Settings


def test_api_requires_key_and_database_before_starting(tmp_path):
    for settings, key in (
        (Settings(), "test-key"),
        (Settings(database_url="postgresql://example.invalid/db"), ""),
    ):
        with pytest.raises(ValueError):
            create_app(settings, token=key, sources_config=tmp_path / "unused.yaml")


def test_http_database_errors_do_not_disclose_credentials(monkeypatch, tmp_path):
    import psycopg

    def unavailable(*args, **kwargs):
        raise psycopg.OperationalError("private-server-message")

    monkeypatch.setattr(psycopg, "connect", unavailable)
    app = create_app(
        Settings(data_dir=tmp_path, database_url="postgresql://user:private-password@example.invalid/db"),
        token="test-key", sources_config=tmp_path / "unused.yaml",
    )
    with TestClient(app) as client:
        response = client.get("/v1/status", headers={"X-AIOps-Key": "test-key"})
        assert response.status_code == 503
        assert response.json() == {"error": "database"}
        assert "private-" not in response.text
