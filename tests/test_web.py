import json
from io import BytesIO
from urllib.error import HTTPError

import pytest
from fastapi.testclient import TestClient

from aiops.web_server import create_web_app


@pytest.fixture
def web():
    client = TestClient(create_web_app(api_url="http://127.0.0.1:9023", token="private-test-key",
        username="operator", password="test-password", session_secret="s"*32))
    assert client.post("/auth/login",json={"username":"operator","password":"test-password"}).status_code==200
    return client


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


def test_login_is_required_and_logout_revokes_existing_cookie():
    app=create_web_app(api_url='http://example.invalid',token='api-secret',username='operator',password='web-secret',session_secret='s'*32)
    client=TestClient(app)
    assert client.get('/',follow_redirects=False).status_code==303
    assert client.get('/read/reports').status_code==401
    assert client.get('/logs/stream?service=fastapi').status_code==401
    assert client.get('/login').status_code==200
    wrong=client.post('/auth/login',json={'username':'operator','password':'wrong'})
    assert wrong.status_code==401
    assert client.post('/auth/login',headers={'Origin':'https://evil.invalid'},json={'username':'operator','password':'web-secret'}).status_code==403
    signed=client.post('/auth/login',json={'username':'operator','password':'web-secret'})
    assert signed.status_code==200
    assert 'HttpOnly' in signed.headers['set-cookie'] and 'SameSite=strict' in signed.headers['set-cookie']
    cookie=client.cookies.get('aiops_session')
    assert client.get('/').status_code==200
    assert 'web-secret' not in client.get('/').text
    assert client.post('/auth/logout').status_code==403
    assert client.post('/auth/logout',headers={'X-AIOps-Web':'1'}).status_code==204
    client.cookies.set('aiops_session',cookie)
    assert client.get('/read/reports').status_code==401


def test_credentials_required_session_expiry_tamper_and_rate_limit():
    from aiops.web_auth import WebAuth
    with pytest.raises(ValueError):create_web_app(api_url='http://example.invalid',token='secret')
    auth=WebAuth('operator','password','s'*32)
    cookie,code=auth.login('operator','password','local')
    assert code==200 and auth.valid(cookie)
    assert not auth.valid(cookie+'bad')
    assert not auth.valid('invalid.é')
    auth.sessions[cookie.split('.')[0]]=0
    assert not auth.valid(cookie)
    for _ in range(10):assert auth.login('wrong','wrong','other')[1]==401
    assert auth.login('operator','password','other')[1]==429


def test_login_cookie_secure_option():
    client=TestClient(create_web_app(api_url='http://example.invalid',token='api',username='a',password='b',session_secret='s'*32,cookie_secure=True),base_url='https://testserver')
    response=client.post('/auth/login',json={'username':'a','password':'b'})
    assert 'Secure' in response.headers['set-cookie']
    assert client.get('/').status_code==200


def test_login_validation_never_echoes_password():
    client=TestClient(create_web_app(api_url='http://example.invalid',token='api',username='a',password='b',session_secret='s'*32))
    response=client.post('/auth/login',json={'username':'a','password':'private-value','unexpected':1})
    assert response.status_code==422 and 'private-value' not in response.text


def test_evaluation_write_requires_session_origin_and_attributes_reviewer(web, monkeypatch):
    requests = []
    class Opener:
        def open(self, request, timeout):
            requests.append(request)
            return BytesIO(json.dumps({'revision': 1}).encode())
    monkeypatch.setattr('aiops.web_server.build_opener', lambda *args: Opener())
    path = '/write/reports/daily-2026-11-04-v0001/evaluations'
    body = {'finding_key': 'report:findings:0', 'expected_revision': 0, 'verdict': 'unconfirmed'}
    assert web.post(path, json=body).status_code == 403
    assert web.post(path, json=body, headers={'X-AIOps-Web': '1', 'Origin': 'https://evil.invalid'}).status_code == 403
    assert not requests
    response = web.post(path, json=body, headers={'X-AIOps-Web': '1'})
    assert response.status_code == 201
    assert json.loads(requests[0].data)['reviewer'] == 'operator'
    assert requests[0].get_header('X-aiops-key') == 'private-test-key'
    assert web.post(path, json={**body, 'reviewer': 'spoof'}, headers={'X-AIOps-Web': '1'}).status_code == 422
    assert web.get('/read/reports/daily-2026-11-04-v0001/evaluations').status_code == 200
    assert web.post('/write/collect', json={}).status_code == 404
    web.post('/auth/logout', headers={'X-AIOps-Web': '1'})
    assert web.post(path, json=body, headers={'X-AIOps-Web': '1'}).status_code == 401
