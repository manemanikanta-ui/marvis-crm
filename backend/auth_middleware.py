"""
auth_middleware.py — Security P1 API-key gate for HTTP + WebSocket (deny-by-default).

MARVIS_API_KEY is referenced by NAME only — its value is never logged, printed,
or echoed. Comparison is constant-time (secrets.compare_digest).

Posture (decided 2026-07-07):
  - RAILWAY (public): deny-by-default. Exempt ONLY the Gmail webhook. /api/_debug/*
    returns 404 unconditionally (key or not). Missing key at startup -> FAIL CLOSED.
  - LOCAL (loopback-only, 127.0.0.1): the dashboard + HUD connect keyless, so the
    local /api surface + dashboard pages + /ws/hud are exempt. The exemption is
    CONDITIONAL on a loopback client: if the backend is bound wider than 127.0.0.1
    and a NON-loopback client connects, the exemption does NOT apply (fail closed)
    and a one-time critical warning is logged.
  - CORS preflight (OPTIONS) is always exempt (it carries no auth by design).
"""
from __future__ import annotations

import os
import logging
import secrets

log = logging.getLogger("marvis.auth")

IS_RAILWAY = bool(os.environ.get("RAILWAY_ENVIRONMENT"))
_API_KEY = os.getenv("MARVIS_API_KEY")  # value referenced by name only; never logged

_WEBHOOK = "/api/gmail/webhook"
_HEALTH = "/api/health"            # Railway healthcheckPath (railway.toml) — must stay keyless
_DEBUG_PREFIX = "/api/_debug"
_RAILWAY_EXEMPT = {_WEBHOOK, _HEALTH}
_LOCAL_EXEMPT_EXACT = {"/", "/dashboard", "/ws/hud", _WEBHOOK, _HEALTH}

if not _API_KEY:
    log.error("SECURITY: MARVIS_API_KEY is not set - protected routes FAIL CLOSED (401).")
elif IS_RAILWAY:
    log.info("SECURITY: API-key gate active (Railway deny-by-default).")

_warned_nonloopback = False


def _is_loopback(scope) -> bool:
    client = scope.get("client")
    if not client:
        return False
    host = client[0] or ""
    return host in ("127.0.0.1", "::1", "localhost") or host.startswith("127.")


def _extract_key(scope) -> "str | None":
    for k, v in scope.get("headers", []):
        if k == b"x-api-key":
            try:
                return v.decode("latin-1")
            except Exception:
                return None
    return None


def _is_exempt(scope, path: str, method: str) -> bool:
    if method == "OPTIONS":          # CORS preflight carries no auth by design
        return True
    if IS_RAILWAY:
        return path in _RAILWAY_EXEMPT  # exact matches (webhook + healthcheck), deny else
    # LOCAL: exemptions apply ONLY to loopback clients (fail closed if bound wider).
    if not _is_loopback(scope):
        return False
    if path in _LOCAL_EXEMPT_EXACT:
        return True
    if path.startswith("/api/"):      # localhost is trusted; whole /api surface open
        return True
    return False


class APIKeyMiddleware:
    """Pure-ASGI so it gates WebSocket upgrades as well as HTTP."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "") or ""
        method = scope.get("method", "") or ""

        # Railway hard-block: the debug hook does not exist here (key or no key).
        if IS_RAILWAY and path.startswith(_DEBUG_PREFIX):
            await self._reject(scope, receive, send, 404, b"Not Found")
            return

        # One-time alert if the local backend is bound wider than loopback.
        global _warned_nonloopback
        if not IS_RAILWAY and not _is_loopback(scope) and not _warned_nonloopback:
            _warned_nonloopback = True
            log.critical("SECURITY: non-loopback client on LOCAL backend - local "
                         "exemptions disabled, enforcing key. Bind host should be 127.0.0.1.")

        if _is_exempt(scope, path, method):
            await self.app(scope, receive, send)
            return

        if not _API_KEY:
            await self._reject(scope, receive, send, 401, b"Unauthorized")
            return

        provided = _extract_key(scope)
        if provided is not None and secrets.compare_digest(provided, _API_KEY):
            await self.app(scope, receive, send)
            return

        await self._reject(scope, receive, send, 401, b"Unauthorized")

    async def _reject(self, scope, receive, send, status: int, body: bytes):
        if scope["type"] == "websocket":
            # Drain the connect event, then reject the handshake.
            try:
                await receive()
            except Exception:
                pass
            await send({"type": "websocket.close", "code": 1008})
            return
        await send({
            "type": "http.response.start",
            "status": status,
            "headers": [
                (b"content-type", b"text/plain; charset=utf-8"),
                (b"content-length", str(len(body)).encode()),
            ],
        })
        await send({"type": "http.response.body", "body": body})
