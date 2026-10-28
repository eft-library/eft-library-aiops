"""Read-only website. The API credential never leaves this server."""

import json
import re
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, ProxyHandler, Request, build_opener

from fastapi import FastAPI
from fastapi import Request as WebRequest
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

ASSETS = Path(__file__).with_name("web")
READ_PATH = re.compile(r"^(status|collections|jobs|reports|reports/[A-Za-z0-9_-]{1,128})$")


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def create_web_app(*, api_url: str, token: str):
    parsed = urlparse(api_url)
    if not token or parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.query or parsed.fragment or parsed.username:
        raise ValueError("Website requires an API URL and AIOPS_API_TOKEN")
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    app.mount("/assets", StaticFiles(directory=ASSETS), name="assets")

    @app.middleware("http")
    async def headers(request, call_next):
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "same-origin"
        response.headers["Content-Security-Policy"] = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
        if request.url.path.startswith("/read/"):
            response.headers["Cache-Control"] = "no-store"
        return response

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
