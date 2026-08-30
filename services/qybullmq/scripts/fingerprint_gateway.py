#!/usr/bin/env python3
"""Per-worker browser fingerprint transport backed by curl_cffi."""

from __future__ import annotations

import argparse
import asyncio
import base64
import http.cookiejar
import json
import signal
from dataclasses import dataclass, field
from typing import Any

from aiohttp import web
from curl_cffi.requests import AsyncSession
from curl_cffi.requests.exceptions import ProxyError, RequestException, SSLError


HOP_BY_HOP = {
    "connection",
    "content-encoding",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "set-cookie",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}


def decode_metadata(value: str | None, fallback: Any) -> Any:
    if not value:
        return fallback
    padding = "=" * (-len(value) % 4)
    return json.loads(base64.urlsafe_b64decode(value + padding).decode("utf-8"))


def encode_metadata(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def serialize_cookies(session: AsyncSession) -> dict[str, Any]:
    cookies = []
    jar = getattr(session.cookies, "jar", session.cookies)
    for cookie in jar:
        cookies.append(
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "secure": bool(cookie.secure),
                "expires": cookie.expires,
            }
        )
    return {"cookies": cookies}


def restore_cookies(session: AsyncSession, state: dict[str, Any] | None) -> None:
    for cookie in (state or {}).get("cookies") or []:
        domain = str(cookie.get("domain") or "")
        path = str(cookie.get("path") or "/")
        session.cookies.jar.set_cookie(
            http.cookiejar.Cookie(
                version=0,
                name=str(cookie.get("name") or ""),
                value=str(cookie.get("value") or ""),
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=bool(domain),
                domain_initial_dot=domain.startswith("."),
                path=path,
                path_specified=True,
                secure=bool(cookie.get("secure")),
                expires=cookie.get("expires"),
                discard=cookie.get("expires") is None,
                comment=None,
                comment_url=None,
                rest={},
                rfc2109=False,
            )
        )


@dataclass
class ProfileSession:
    profile_id: str
    engine: str
    impersonate_target: str
    user_agent: str
    proxy_url: str
    session: AsyncSession
    semaphore: asyncio.Semaphore
    max_clients: int
    reset_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def close(self) -> None:
        await self.session.close()

    async def reset_after_failure(self, failed_session: AsyncSession) -> bool:
        async with self.reset_lock:
            if self.session is not failed_session:
                return False
            cookie_state = serialize_cookies(failed_session)
            replacement = AsyncSession(max_clients=self.max_clients)
            restore_cookies(replacement, cookie_state)
            self.session = replacement
            await failed_session.close()
            return True


def curl_error_code(error: RequestException) -> int | None:
    try:
        return int(getattr(error, "code", None))
    except (TypeError, ValueError):
        return None


def transport_failure_kind(error: RequestException) -> str:
    if isinstance(error, (ProxyError, SSLError)):
        return "proxy_transport"
    return "upstream_transient"


def valid_target_http_status(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and 200 <= value <= 599


def json_evidence(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return repr(value)


def response_body_sample_base64(response: Any, limit: int = 2048) -> str:
    content = getattr(response, "content", b"")
    if isinstance(content, str):
        content = content.encode("utf-8", errors="replace")
    elif not isinstance(content, (bytes, bytearray)):
        content = repr(content).encode("utf-8", errors="replace")
    return base64.b64encode(bytes(content[:limit])).decode("ascii")


class Gateway:
    def __init__(self) -> None:
        self.profiles: dict[str, ProfileSession] = {}

    async def configure(self, request: web.Request) -> web.Response:
        payload = await request.json()
        proxy_url = str(payload.get("proxy_url") or "")
        if not proxy_url:
            raise web.HTTPBadRequest(text="proxy_url is required")
        desired_ids = set()
        for profile in payload.get("profiles") or []:
            profile_id = str(profile.get("profile_id") or "")
            if not profile_id:
                raise web.HTTPBadRequest(text="profile_id is required")
            desired_ids.add(profile_id)
            current = self.profiles.pop(profile_id, None)
            if current:
                await current.close()
            max_clients = max(1, min(int(profile.get("max_connections") or 1), 4))
            session = AsyncSession(max_clients=max_clients)
            restore_cookies(session, profile.get("cookie_state"))
            self.profiles[profile_id] = ProfileSession(
                profile_id=profile_id,
                engine=str(profile.get("engine") or ""),
                impersonate_target=str(profile.get("impersonate_target") or "chrome"),
                user_agent=str(profile.get("user_agent") or ""),
                proxy_url=proxy_url,
                session=session,
                semaphore=asyncio.Semaphore(max_clients),
                max_clients=max_clients,
            )
        for stale_id in set(self.profiles) - desired_ids:
            stale = self.profiles.pop(stale_id)
            await stale.close()
        return web.json_response({"ok": True, "profiles": len(self.profiles)})

    async def fetch(self, request: web.Request) -> web.Response:
        profile_id = request.match_info["profile_id"]
        profile = self.profiles.get(profile_id)
        if not profile:
            raise web.HTTPNotFound(text="profile not configured")
        url = str(decode_metadata(request.headers.get("x-fingerprint-url"), ""))
        if not url.startswith(("https://", "http://")):
            raise web.HTTPBadRequest(text="only HTTP(S) fingerprint requests are allowed")
        method = str(request.headers.get("x-fingerprint-method") or "GET").upper()
        headers = decode_metadata(request.headers.get("x-fingerprint-headers"), {})
        headers["User-Agent"] = profile.user_agent
        timeout = max(1.0, int(request.headers.get("x-fingerprint-timeout-ms") or "30000") / 1000)
        allow_redirects = request.headers.get("x-fingerprint-redirect", "follow") != "manual"
        body = await request.read()
        async with profile.semaphore:
            active_session = profile.session
            try:
                response = await active_session.request(
                    method,
                    url,
                    headers=headers,
                    data=body if body else None,
                    proxy=profile.proxy_url,
                    impersonate=profile.impersonate_target,
                    timeout=timeout,
                    allow_redirects=allow_redirects,
                )
            except RequestException as error:
                session_reset = await profile.reset_after_failure(active_session)
                return web.json_response(
                    {
                        "error": "fingerprint transport request failed",
                        "error_type": type(error).__name__,
                        "curl_code": curl_error_code(error),
                        "failure_kind": transport_failure_kind(error),
                        "session_reset": session_reset,
                    },
                    status=502,
                )
        if not valid_target_http_status(response.status_code):
            response_headers = {
                str(name): str(value)
                for name, value in response.headers.items()
                if str(name).lower() not in HOP_BY_HOP
            }
            return web.json_response(
                {
                    "error": "fingerprint target returned an invalid HTTP status",
                    "error_type": "InvalidTargetHttpStatus",
                    "failure_kind": "invalid_target_status",
                    "target_status_raw": json_evidence(response.status_code),
                    "target_url": url,
                    "target_response_headers": response_headers,
                    "target_body_sample_base64": response_body_sample_base64(response),
                },
                status=502,
            )
        response_headers = {
            str(name): str(value)
            for name, value in response.headers.items()
            if str(name).lower() not in HOP_BY_HOP
        }
        return web.Response(
            status=200,
            body=response.content,
            headers={
                "content-type": response_headers.get("content-type", "application/octet-stream"),
                "x-fingerprint-response-status": str(response.status_code),
                "x-fingerprint-response-headers": encode_metadata(response_headers),
            },
        )

    async def snapshot(self, request: web.Request) -> web.Response:
        profile = self.profiles.get(request.match_info["profile_id"])
        if not profile:
            raise web.HTTPNotFound(text="profile not configured")
        return web.json_response(serialize_cookies(profile.session))

    async def close(self) -> None:
        profiles = list(self.profiles.values())
        self.profiles.clear()
        await asyncio.gather(*(profile.close() for profile in profiles), return_exceptions=True)


async def run(host: str, port: int) -> None:
    gateway = Gateway()
    stop = asyncio.Event()
    app = web.Application(client_max_size=16 * 1024 * 1024)
    app.router.add_get("/health", lambda _request: web.json_response({"ok": True}))
    app.router.add_post("/v1/profiles/configure", gateway.configure)
    app.router.add_post("/v1/fetch/{profile_id}", gateway.fetch)
    app.router.add_get("/v1/profiles/{profile_id}/snapshot", gateway.snapshot)

    async def shutdown(_request: web.Request) -> web.Response:
        stop.set()
        return web.json_response({"ok": True})

    app.router.add_post("/shutdown", shutdown)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass
    await stop.wait()
    await gateway.close()
    await runner.cleanup()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=3099)
    args = parser.parse_args()
    asyncio.run(run(args.host, args.port))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
