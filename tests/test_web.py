import json
from io import BytesIO
from urllib.error import HTTPError

import pytest
from fastapi.testclient import TestClient

from aiops.web_server import create_web_app


@pytest.fixture
def web():
    return TestClient(create_web_app(api_url="http://127.0.0.1:9023", token="private-test-key"))


def test_website_serves_assets_without_embedding_credentials(web):
    assert web.get("/health").json() == {"status": "ready"}
    for path in ["/", "/assets/app.js", "/assets/style.css"]:
        response = web.get(path)
        assert response.status_code == 200
        assert "private-test-key" not in response.text
        assert "Content-Security-Policy" in response.headers


def test_web_proxy_is_fixed_get_only_and_keeps_key_on_server(web, monkeypatch):
    requests = []

    class Opener:
        def open(self, request, timeout):
            requests.append(request)
            return BytesIO(json.dumps({"items": [], "total": 0}).encode())

    monkeypatch.setattr("aiops.web_server.build_opener", lambda *args: Opener())
    assert web.get("/read/reports?type=weekly&page=2").json()["total"] == 0
    assert requests[0].full_url == "http://127.0.0.1:9023/v1/reports?type=weekly&page=2"
    assert requests[0].get_method() == "GET"
    assert requests[0].get_header("X-aiops-key") == "private-test-key"
    assert web.get("/read/reports/daily-2026-10-27-v0001").status_code == 200
    for path in ["collect", "worker/run-once", "jobs/submit", "db/init"]:
        assert web.get("/read/" + path).status_code == 404
    assert web.post("/read/reports", json={"type": "daily"}).status_code == 405
    assert len(requests) == 2
    assert web.get("/read/reports").headers["cache-control"] == "no-store"


def test_web_proxy_sanitizes_authentication_errors(web, monkeypatch):
    class Opener:
        def open(self, request, timeout):
            raise HTTPError(request.full_url, 401, "private-server-text", {}, None)

    monkeypatch.setattr("aiops.web_server.build_opener", lambda *args: Opener())
    response = web.get("/read/status")
    assert response.status_code == 401
    assert response.json() == {"error": "api_authentication"}
    assert "private-" not in response.text


def test_favicon_uses_project_original_with_packaged_fallback(web, tmp_path, monkeypatch):
    from aiops.web_server import ASSETS

    monkeypatch.chdir(tmp_path)
    bundled = web.get("/favicon.ico")
    assert bundled.status_code == 200
    assert bundled.content == (ASSETS / "favicon.ico").read_bytes()
    assert bundled.headers["cache-control"] == "no-cache"
    source = tmp_path / "favicon.ico"
    source.write_bytes(bundled.content + b"source-marker")
    assert web.get("/favicon.ico").content == source.read_bytes()
    assert '/favicon.ico' in web.get("/").text
