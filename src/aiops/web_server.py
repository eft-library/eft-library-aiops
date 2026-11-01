"""Read-only website. The API credential never leaves this server."""

import json
import re
import asyncio
import threading
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse, urlencode
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from fastapi import FastAPI
from fastapi import Request as WebRequest
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse, Response
from pydantic import BaseModel, ConfigDict, Field
from aiops.web_auth import WebAuth, COOKIE
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError

ASSETS = Path(__file__).with_name("web")
READ_PATH = re.compile(r"^(status|collections|jobs|reports|reports/[A-Za-z0-9_-]{1,128}|logs/services|logs/files)$")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def create_web_app(*, api_url: str, token: str, username: str = "", password: str = "", session_secret: str = "", cookie_secure: bool = False):
    parsed = urlparse(api_url)
    if not token or parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment or parsed.username:
        raise ValueError("Website requires an API URL and AIOPS_API_TOKEN")
    auth = WebAuth(username, password, session_secret)
    stream_slots = threading.BoundedSemaphore(8)
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    @app.exception_handler(RequestValidationError)
    async def invalid_input(request, exc):
        return JSONResponse(status_code=422,content={"error":"invalid_request"})

    app.mount("/assets", StaticFiles(directory=ASSETS), name="assets")

    @app.middleware("http")
    async def headers(request, call_next):
        public_paths = {"/health", "/login", "/auth/login", "/favicon.ico", "/assets/style.css", "/assets/login.js"}
        if request.url.path not in public_paths and not auth.valid(request.cookies.get(COOKIE)):
            response = JSONResponse(status_code=401, content={"error":"login_required"}) if request.url.path.startswith(("/read/", "/logs/", "/auth/")) else RedirectResponse("/login", status_code=303)
        else:
            response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
        response.headers["Cache-Control"] = "no-cache" if request.url.path == "/favicon.ico" else "no-store"
        response.headers["X-Frame-Options"] = "DENY"
        return response

    class Credentials(BaseModel):
        model_config = ConfigDict(extra="forbid")
        username: str = Field(min_length=1, max_length=128)
        password: str = Field(min_length=1, max_length=1024)

    def same_origin(request):
        origin = request.headers.get("origin")
        return not origin or urlparse(origin).netloc == request.headers.get("host")

    @app.get("/login")
    def login_page():
        return FileResponse(ASSETS / "login.html")

    @app.post("/auth/login")
    def login(body: Credentials, request: WebRequest):
        if not same_origin(request):
            return JSONResponse(status_code=403, content={"error":"invalid_origin"})
        cookie, code = auth.login(body.username,body.password,request.client.host if request.client else "unknown")
        response = JSONResponse(status_code=code, content={"status":"authenticated"} if cookie else {"error":"login_failed" if code==401 else "too_many_attempts"})
        if cookie:
            auth.logout(request.cookies.get(COOKIE))
            response.set_cookie(COOKIE,cookie,max_age=8*3600,httponly=True,secure=cookie_secure,samesite="strict",path="/")
        return response

    @app.post("/auth/logout")
    def logout(request: WebRequest):
        if not same_origin(request) or request.headers.get("x-aiops-web")!="1":
            return JSONResponse(status_code=403,content={"error":"invalid_origin"})
        auth.logout(request.cookies.get(COOKIE))
        response=Response(status_code=204)
        response.delete_cookie(COOKIE,path="/",secure=cookie_secure,httponly=True,samesite="strict")
        return response

    def upstream_json(path, query=""):
        upstream=Request(api_url.rstrip("/")+"/v1/"+path+("?"+query if query else ""),headers={"X-AIOps-Key":token},method="GET")
        with build_opener(ProxyHandler({}),NoRedirect()).open(upstream,timeout=10) as response:
            raw=response.read(2_000_001)
            if len(raw)>2_000_000:raise ValueError("response too large")
            return json.loads(raw)

    @app.get("/logs/stream")
    async def log_stream(request: WebRequest):
        params=dict(request.query_params)
        if set(params)-{"service","file_id","cursor"} or not params.get("service") or len(params.get("service",""))>64 or len(params.get("cursor",""))>1024 or (params.get("file_id") and not re.fullmatch(r"[a-f0-9]{32}",params["file_id"])):
            return JSONResponse(status_code=422,content={"error":"invalid_log_request"})
        if not stream_slots.acquire(blocking=False):
            return JSONResponse(status_code=429,content={"error":"too_many_streams"})
        cookie=request.cookies.get(COOKIE)
        async def events():
            started=time.monotonic()
            try:
                while time.monotonic()-started<60 and auth.valid(cookie):
                    if await request.is_disconnected():break
                    try:
                        payload=await asyncio.to_thread(upstream_json,"logs/tail",urlencode(params))
                        if not auth.valid(cookie):break
                        params['cursor']=payload['cursor']
                        yield 'data: '+json.dumps({'event':'logs',**payload},ensure_ascii=False)+'\n\n'
                    except (OSError,URLError,ValueError,KeyError):
                        yield 'data: '+json.dumps({'event':'error','error':'logs_unavailable'})+'\n\n'
                        break
                    await asyncio.sleep(1)
            finally:
                stream_slots.release()
        return StreamingResponse(events(),media_type="text/event-stream",headers={"Cache-Control":"no-store","X-Accel-Buffering":"no"})

    @app.get("/favicon.ico")
    def favicon():
        # The project-root icon is the user's source; the wheel has a bundled fallback.
        original = Path.cwd() / "favicon.ico"
        return FileResponse(original if original.is_file() else ASSETS / "favicon.ico",
                            media_type="image/x-icon", headers={"Cache-Control": "no-cache"})

    @app.get("/health")
    def health():
        return {"status": "ready"}

    @app.get("/")
    def index():
        return FileResponse(ASSETS / "index.html", headers={"Cache-Control": "no-cache"})

    @app.get("/read/{path:path}")
    def read_api(path: str, request: WebRequest):
        # A fixed GET allowlist: this proxy can never submit jobs or run a worker.
        if not READ_PATH.fullmatch(path):
            return JSONResponse(status_code=404, content={"error": "not_found"})
        target = api_url.rstrip("/") + "/v1/" + path
        if request.url.query:
            target += "?" + request.url.query
        upstream = Request(target, headers={"X-AIOps-Key": token}, method="GET")
        try:
            with build_opener(ProxyHandler({}), NoRedirect()).open(upstream, timeout=10) as response:
                raw = response.read(2_000_001)
                if len(raw) > 2_000_000:
                    raise ValueError("response too large")
                return JSONResponse(content=json.loads(raw))
        except HTTPError as exc:
            codes = {401: "api_authentication", 404: "not_found", 409: "busy", 422: "invalid_filter"}
            return JSONResponse(status_code=exc.code if exc.code in codes else 502,
                content={"error": codes.get(exc.code, "api_unavailable")})
        except (OSError, URLError, ValueError):
            return JSONResponse(status_code=502, content={"error": "api_unavailable"})

    return app
