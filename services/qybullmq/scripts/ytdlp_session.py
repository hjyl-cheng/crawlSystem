#!/usr/bin/env python3
"""Long-lived yt-dlp engine for one BullMQ channel worker.

The process owns one primary YoutubeDL instance and handles one leased channel at
a time over a JSON-lines protocol. A profile and its Rota proxy are configured
together, then the process and cookie jar are reused across channel jobs.
"""

from __future__ import annotations

import base64
import datetime
import http.cookiejar
import json
import os
import random
import re
import string
import sys
import time
import unicodedata
from typing import Any


def failure_kind(message: Any) -> str:
    lowered = str(message).lower()
    if (
        re.search(r"\b404\b", lowered)
        or any(
            token in lowered
            for token in ("does not exist", "private video", "video is private", "has been removed")
        )
        or re.search(r"(?:channel|video|playlist).{0,80}not found", lowered)
    ):
        return "content_terminal"
    if any(
        token in lowered
        for token in (
            "proxyerror", "proxy_unavailable", "proxy connection", "tunnel connection",
            "socks4 connection", "socks5 connection", "wrong_version_number",
            "wrong version number", "proxy authentication",
        )
    ) or re.search(r"\b407\b", lowered):
        return "proxy_transport"
    if re.search(r"\b429\b", lowered) or "too many requests" in lowered or "rate limit" in lowered:
        return "youtube_rate_limited"
    if any(
        token in lowered
        for token in ("not a bot", "bot challenge", "captcha", "unusual traffic", "verify you are human")
    ):
        return "youtube_challenge"
    if (
        re.search(r"\b403\b", lowered)
        or any(
            token in lowered
            for token in (
                "forbidden", "login required", "sign in", "po token", "po_token",
                "proof of origin", "not available on this app", "client is not supported",
                "no video formats", "requested format is not available",
                "failed to extract any player response", "no player response",
            )
        )
    ):
        return "token_or_client"
    if (
        re.search(r"\b(?:408|425|5\d\d)\b", lowered)
        or any(
            token in lowered
            for token in (
                "timeout", "timed out", "connection reset", "connection refused", "network",
                "socket", "econn", "eai_again", "temporary failure in name resolution",
                "name or service not known", "dns", "remote end closed",
            )
        )
    ):
        return "upstream_transient"
    return "unknown"


if __name__ == "__main__" and sys.argv[1:] == ["--classify-failure"]:
    messages = json.load(sys.stdin)
    print(json.dumps([failure_kind(message) for message in messages]))
    raise SystemExit(0)


import yt_dlp
from yt_dlp.networking.impersonate import ImpersonateTarget

from ytdlp_comments import (
    bounded_youtube_extractor_args,
    comment_limit_from_env,
    install_original_comment_count_capture,
    normalize_ytdlp_comment_page,
    original_comment_count,
)


def monotonic_ms() -> int:
    return round(time.monotonic() * 1000)


def generate_visitor_data() -> str:
    chars = string.ascii_letters + string.digits + "_-"
    visitor_id = "".join(random.choices(chars, k=11))
    timestamp = int(time.time())
    id_bytes = visitor_id.encode()
    field1 = bytes([0x0A, len(id_bytes)]) + id_bytes

    def varint(value: int) -> bytes:
        output = []
        while value > 0x7F:
            output.append((value & 0x7F) | 0x80)
            value >>= 7
        output.append(value)
        return bytes(output)

    encoded = field1 + bytes([0x28]) + varint(timestamp)
    return base64.urlsafe_b64encode(encoded).decode().rstrip("=")


def iso_from_upload_date(value: Any) -> str | None:
    if isinstance(value, str) and value.isdigit() and len(value) == 8:
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return None


def iso_from_timestamp(value: Any) -> str | None:
    try:
        if value is None:
            return None
        parsed = datetime.datetime.fromtimestamp(int(value), datetime.timezone.utc)
        return parsed.isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def best_thumbnail(info: dict[str, Any]) -> str | None:
    thumbnails = info.get("thumbnails")
    if isinstance(thumbnails, list) and thumbnails:
        valid = [item for item in thumbnails if isinstance(item, dict) and item.get("url")]
        if valid:
            valid.sort(
                key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0),
                reverse=True,
            )
            return str(valid[0].get("url"))
    value = info.get("thumbnail")
    return str(value) if value else None


def best_avatar(info: dict[str, Any]) -> str | None:
    thumbnails = info.get("thumbnails")
    if not isinstance(thumbnails, list):
        value = info.get("thumbnail")
        return str(value) if value else None
    valid = [item for item in thumbnails if isinstance(item, dict) and item.get("url")]
    avatars = [
        item
        for item in valid
        if "avatar" in str(item.get("id", "")).lower()
        or (
            int(item.get("width") or 0) > 0
            and int(item.get("width") or 0) == int(item.get("height") or 0)
        )
    ]
    pool = avatars or valid
    if not pool:
        value = info.get("thumbnail")
        return str(value) if value else None
    pool.sort(
        key=lambda item: int(item.get("width") or 0) * int(item.get("height") or 0),
        reverse=True,
    )
    return str(pool[0].get("url"))


def text_value(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if isinstance(value.get("simpleText"), str):
            return value["simpleText"]
        if isinstance(value.get("content"), str):
            return value["content"]
        if isinstance(value.get("runs"), list):
            return "".join(
                str(item.get("text") or "")
                for item in value["runs"]
                if isinstance(item, dict)
            )
    return ""


def folded(value: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(value or "")).lower()
    return "".join(char for char in normalized if not unicodedata.combining(char))


def parse_count(value: Any) -> int | None:
    raw = text_value(value).replace("\u00a0", " ").strip().lower()
    match = re.search(
        r"(\d[\d.,\s]*)(?:\s*)(thousand|millions?|million|mil|mi|bilhao|bilhoes|billion|bi|[kmb])?",
        folded(raw),
    )
    if not match:
        return None
    number_text = match.group(1).strip()
    unit = (match.group(2) or "").lower()
    try:
        if unit:
            normalized = number_text.replace(" ", "")
            if "," in normalized and "." not in normalized:
                normalized = normalized.replace(",", ".")
            else:
                normalized = normalized.replace(",", "")
            number = float(normalized)
            if unit in {"k", "mil", "thousand"}:
                multiplier = 1_000
            elif unit in {"m", "mi", "million", "millions"}:
                multiplier = 1_000_000
            else:
                multiplier = 1_000_000_000
            return int(number * multiplier)
        digits = re.sub(r"\D", "", number_text)
        return int(digits) if digits else None
    except (TypeError, ValueError):
        return None


def comment_state_from_initial_data(
    initial_data: Any,
    info: dict[str, Any],
) -> tuple[int | None, bool | None, str, str | None]:
    if info.get("comment_count") is not None:
        return int(info["comment_count"]), False, "exact", "yt_dlp"
    if not isinstance(initial_data, dict):
        return None, None, "unresolved", None

    stack = [initial_data]
    has_comment_surface = False
    explicit_disabled = False
    parsed_count = None
    while stack:
        value = stack.pop()
        if isinstance(value, dict):
            for key, item in value.items():
                lowered_key = key.lower()
                if key in {
                    "commentsEntryPointHeaderRenderer",
                    "commentsHeaderRenderer",
                    "commentThreadRenderer",
                }:
                    has_comment_surface = True
                if key == "commentsHeaderRenderer" and isinstance(item, dict):
                    parsed_count = parse_count(item.get("countText"))
                if key == "panelIdentifier" and "comment" in str(item).lower():
                    has_comment_surface = True
                if "commentsdisabled" in lowered_key and item is True:
                    explicit_disabled = True
                if key == "messageRenderer":
                    message = folded(text_value(item.get("text")) if isinstance(item, dict) else item)
                    mentions_comments = "comment" in message or "coment" in message
                    disabled_words = any(
                        token in message
                        for token in (
                            "turned off",
                            "disabled",
                            "desativad",
                            "deshabilitad",
                            "desactivad",
                            "desactive",
                        )
                    )
                    if mentions_comments and disabled_words:
                        explicit_disabled = True
                stack.append(item)
        elif isinstance(value, list):
            stack.extend(value)

    if parsed_count is not None:
        return parsed_count, False, "exact", "yt_dlp_initial_data"
    if explicit_disabled:
        return 0, True, "disabled", "yt_dlp_initial_data"
    if has_comment_surface:
        return 0, False, "zero_from_surface", "yt_dlp_initial_data"
    return None, None, "unresolved", None


def find_live_broadcast_details(value: Any) -> list[dict[str, Any]]:
    found = []
    stack = [value]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            details = current.get("liveBroadcastDetails")
            if isinstance(details, dict):
                found.append(details)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return found


def player_content_type_signals(player_responses: Any) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    stack = [player_responses]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if isinstance(current.get("videoDetails"), dict) or isinstance(current.get("microformat"), dict):
                candidates.append(current)
            else:
                stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(reversed(current))

    def merged_boolean(*paths: tuple[str, ...]) -> bool | None:
        observed: list[bool] = []
        for candidate in candidates:
            for path in paths:
                value: Any = candidate
                for key in path:
                    value = value.get(key) if isinstance(value, dict) else None
                if isinstance(value, bool):
                    observed.append(value)
        if True in observed:
            return True
        return False if False in observed else None

    canonical_url = None
    has_live_broadcast_details = False
    for candidate in candidates:
        microformat = candidate.get("microformat", {}).get("playerMicroformatRenderer", {})
        if not isinstance(microformat, dict):
            microformat = {}
        if canonical_url is None:
            canonical_url = microformat.get("canonicalUrl") or candidate.get("canonicalUrl")
        if isinstance(microformat.get("liveBroadcastDetails"), dict):
            has_live_broadcast_details = True

    return {
        "source": "youtube_watch_ytdlp_player",
        "canonical_url": canonical_url,
        "is_shorts_eligible": merged_boolean(
            ("microformat", "playerMicroformatRenderer", "isShortsEligible"),
            ("videoDetails", "isShortsEligible"),
        ),
        "is_live_content": merged_boolean(("videoDetails", "isLiveContent")),
        "is_live": merged_boolean(("videoDetails", "isLive")),
        "is_upcoming": merged_boolean(("videoDetails", "isUpcoming")),
        "is_live_now": merged_boolean(
            ("microformat", "playerMicroformatRenderer", "liveBroadcastDetails", "isLiveNow"),
        ),
        "has_live_broadcast_details": has_live_broadcast_details,
    }


def player_playability(player_responses: Any) -> dict[str, Any]:
    statuses: list[dict[str, Any]] = []
    stack = [player_responses]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            status = current.get("playabilityStatus")
            if isinstance(status, dict):
                statuses.append(status)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(reversed(current))
    if not statuses:
        return {"status": None, "reason": None}
    playable = next((item for item in statuses if str(item.get("status") or "").upper() == "OK"), None)
    selected = playable or max(
        statuses,
        key=lambda item: len(str(item.get("reason") or item.get("messages") or "")),
    )
    reason = selected.get("reason")
    if not reason and isinstance(selected.get("messages"), list):
        reason = " ".join(str(value) for value in selected["messages"] if value)
    return {"status": selected.get("status"), "reason": reason}


def capture_initial_extract_result(result: Any) -> dict[str, Any]:
    values = list(result) if isinstance(result, (list, tuple)) else [result]

    def is_player_response(value: Any) -> bool:
        if isinstance(value, dict):
            return any(key in value for key in ("videoDetails", "playabilityStatus", "streamingData"))
        return isinstance(value, list) and any(is_player_response(item) for item in value)

    def is_initial_data(value: Any) -> bool:
        return isinstance(value, dict) and any(key in value for key in (
            "contents", "engagementPanels", "onResponseReceivedEndpoints",
            "onResponseReceivedActions", "frameworkUpdates",
        ))

    return {
        "webpage": next((value for value in values if isinstance(value, str)), None),
        "player_responses": next((value for value in values if is_player_response(value)), None),
        "initial_data": next((value for value in values if is_initial_data(value)), None),
    }


def availability_from_player(player_responses: Any, fallback: Any) -> str | None:
    fallback_text = str(fallback or "").lower()
    if fallback_text in {"private", "subscriber_only", "premium_only", "unlisted", "age_restricted"}:
        return fallback_text
    stack = [player_responses]
    statuses = []
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            status = current.get("playabilityStatus")
            if isinstance(status, dict):
                statuses.append(status)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    for status in statuses:
        status_text = folded(json.dumps(status, ensure_ascii=False))
        if "member" in status_text or "subscriber" in status_text or "membro" in status_text:
            return "subscriber_only"
        code = str(status.get("status") or "").upper()
        if code == "OK":
            return "public"
        if code == "LOGIN_REQUIRED":
            return "needs_auth"
    return None


def missing_tab_error(message: Any) -> bool:
    lowered = str(message).lower()
    return "does not have a" in lowered and "tab" in lowered


def missing_uploads_error(message: Any) -> bool:
    lowered = str(message).lower()
    return "http error 404" in lowered or "playlist does not exist" in lowered


def cookie_state(jar: Any) -> dict[str, Any]:
    return {
        "cookies": [
            {
                "name": cookie.name,
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "secure": bool(cookie.secure),
                "expires": cookie.expires,
            }
            for cookie in jar
        ]
    }


def restore_cookie_state(jar: Any, state: dict[str, Any] | None) -> None:
    for item in (state or {}).get("cookies") or []:
        domain = str(item.get("domain") or "")
        path = str(item.get("path") or "/")
        jar.set_cookie(
            http.cookiejar.Cookie(
                version=0,
                name=str(item.get("name") or ""),
                value=str(item.get("value") or ""),
                port=None,
                port_specified=False,
                domain=domain,
                domain_specified=bool(domain),
                domain_initial_dot=domain.startswith("."),
                path=path,
                path_specified=True,
                secure=bool(item.get("secure")),
                expires=item.get("expires"),
                discard=item.get("expires") is None,
                comment=None,
                comment_url=None,
                rest={},
                rfc2109=False,
            )
        )


class PersistentYtDlp:
    def __init__(self) -> None:
        self.proxy = None
        self.language = os.environ.get("YOUTUBE_LANGUAGE") or "pt-BR"
        self.profile_id = None
        self.impersonate_target = "safari"
        self.user_agent = ""
        self.visitor_data = None
        self.timezone = os.environ.get("TZ") or "America/Sao_Paulo"
        self.primary = None
        self.fallback = None
        self._fallback_cookie_state = {"cookies": []}
        self.capture: dict[str, Any] = {}
        self.active_channel: str | None = None
        self.lease_id: str | None = None
        self.channels_processed = 0
        self.commands_processed = 0
        self.created_at = time.time()

    def _options(self, client: str) -> dict[str, Any]:
        youtube_args: dict[str, list[str]] = {"player_client": [client]}
        if client != "mweb" and self.visitor_data:
            youtube_args["visitor_data"] = [self.visitor_data]
        options: dict[str, Any] = {
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "extract_flat": "in_playlist",
            "playlistend": 100,
            "ignoreerrors": False,
            "ignore_no_formats_error": True,
            "socket_timeout": 25,
            "extractor_args": {"youtube": youtube_args},
            "http_headers": {
                "Accept-Language": f"{self.language},pt;q=0.9,en;q=0.8",
                "User-Agent": self.user_agent,
            },
            "impersonate": ImpersonateTarget.from_str(self.impersonate_target),
        }
        if self.proxy:
            options["proxy"] = self.proxy
        return options

    def _new_ydl(self, client: str) -> yt_dlp.YoutubeDL:
        return yt_dlp.YoutubeDL(self._options(client))

    def configure(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self.lease_id:
            raise RuntimeError("cannot reconfigure yt-dlp while a channel lease is active")
        self.close()
        self.proxy = str(payload.get("proxy_url") or "") or None
        self.profile_id = str(payload.get("profile_id") or "") or None
        self.impersonate_target = str(payload.get("impersonate_target") or "safari")
        self.user_agent = str(payload.get("user_agent") or "")
        self.visitor_data = str(payload.get("visitor_data") or "") or generate_visitor_data()
        self.language = str(payload.get("language") or self.language)
        self.timezone = str(payload.get("timezone") or self.timezone)
        os.environ["TZ"] = self.timezone
        if hasattr(time, "tzset"):
            time.tzset()
        if not self.proxy or not self.profile_id or not self.user_agent:
            raise ValueError("yt-dlp fingerprint profile requires proxy_url, profile_id and user_agent")
        cookies = payload.get("cookie_state") or {}
        self.primary = self._new_ydl("web_safari")
        restore_cookie_state(self.primary.cookiejar, cookies.get("primary") or cookies)
        self.fallback = None
        self._fallback_cookie_state = cookies.get("fallback") or {"cookies": []}
        self._install_capture(self.primary)
        return self.stats()

    def _install_capture(self, ydl: yt_dlp.YoutubeDL) -> None:
        install_original_comment_count_capture(ydl)
        extractor = ydl.get_info_extractor("Youtube")
        original = extractor._initial_extract

        def capture_initial_data(*args: Any, **kwargs: Any) -> Any:
            result = original(*args, **kwargs)
            self.capture = capture_initial_extract_result(result)
            return result

        extractor._initial_extract = capture_initial_data

    def _fallback_ydl(self) -> yt_dlp.YoutubeDL:
        if self.fallback is None:
            self.fallback = self._new_ydl("android")
            restore_cookie_state(self.fallback.cookiejar, self._fallback_cookie_state)
            self._install_capture(self.fallback)
        return self.fallback

    def acquire(self, lease_id: str, channel_id: str, language: str) -> dict[str, Any]:
        if self.lease_id and self.lease_id != lease_id:
            raise RuntimeError(f"yt-dlp engine already leased by {self.active_channel}")
        if language and language != self.language:
            self.language = language
            for ydl in (self.primary, self.fallback):
                if ydl is not None:
                    ydl.params["http_headers"] = {
                        "Accept-Language": f"{self.language},pt;q=0.9,en;q=0.8",
                        "User-Agent": self.user_agent,
                    }
        self.lease_id = lease_id
        self.active_channel = channel_id
        return self.stats()

    def release(self, lease_id: str) -> dict[str, Any]:
        if self.lease_id and lease_id != self.lease_id:
            raise RuntimeError("yt-dlp lease mismatch")
        if self.lease_id:
            self.channels_processed += 1
        self.lease_id = None
        self.active_channel = None
        return {
            **self.stats(),
            "cookie_state": {
                "primary": cookie_state(self.primary.cookiejar) if self.primary else {"cookies": []},
                "fallback": cookie_state(self.fallback.cookiejar) if self.fallback else self._fallback_cookie_state,
            },
        }

    def require_lease(self, lease_id: str) -> None:
        if not self.lease_id or lease_id != self.lease_id:
            raise RuntimeError("yt-dlp command requires the active channel lease")

    def _extract(
        self,
        ydl: yt_dlp.YoutubeDL,
        url: str,
        playlist_end: int | None = None,
        parameter_overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        previous_end = ydl.params.get("playlistend")
        missing = object()
        previous_parameters = {
            key: ydl.params.get(key, missing)
            for key in (parameter_overrides or {})
        }
        if playlist_end is not None:
            ydl.params["playlistend"] = playlist_end
        ydl.params.update(parameter_overrides or {})
        try:
            result = ydl.extract_info(url, download=False) or {}
            if not isinstance(result, dict):
                return {}
            return result
        finally:
            ydl.params["playlistend"] = previous_end
            for key, value in previous_parameters.items():
                if value is missing:
                    ydl.params.pop(key, None)
                else:
                    ydl.params[key] = value

    @staticmethod
    def _comment_detail_options(ydl: yt_dlp.YoutubeDL) -> dict[str, Any]:
        extractor_args = dict(ydl.params.get("extractor_args") or {})
        extractor_args["youtube"] = bounded_youtube_extractor_args(
            dict(extractor_args.get("youtube") or {}),
            comment_limit_from_env(),
        )
        return {
            "extractor_args": extractor_args,
            "getcomments": True,
            # Comment collection is best-effort and must not erase valid detail metadata.
            "ignoreerrors": True,
        }

    def channel_metadata(self, lease_id: str, url: str) -> dict[str, Any]:
        self.require_lease(lease_id)
        started = monotonic_ms()
        info = self._extract(self.primary, url, playlist_end=1)
        self.commands_processed += 1
        return {
            "ok": True,
            "id": info.get("id"),
            "title": info.get("title") or info.get("channel") or info.get("uploader"),
            "channel": info.get("channel"),
            "channel_id": info.get("channel_id"),
            "channel_url": info.get("channel_url"),
            "uploader": info.get("uploader"),
            "uploader_id": info.get("uploader_id"),
            "uploader_url": info.get("uploader_url"),
            "description": info.get("description"),
            "avatar_url": best_avatar(info),
            "channel_follower_count": info.get("channel_follower_count"),
            "webpage_url": info.get("webpage_url"),
            "engine": "persistent",
            "duration_ms": monotonic_ms() - started,
        }

    def _extract_flat(
        self,
        url: str,
        limit: int,
        allow_missing_tab: bool = False,
    ) -> tuple[list[dict[str, Any]], str | None, dict[str, Any]]:
        started = monotonic_ms()
        last_error = None
        attempts = 0
        for attempt in range(2):
            attempts = attempt + 1
            try:
                info = self._extract(self.primary, url, playlist_end=limit)
                source_entries = info.get("entries") or []
                entries = [
                    entry
                    for entry in source_entries
                    if isinstance(entry, dict) and entry.get("id")
                ]
                return entries, None, {
                    "attempts": attempts,
                    "duration_ms": monotonic_ms() - started,
                    "entry_count": len(entries),
                    "parse_gap_count": len(source_entries) - len(entries),
                }
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                kind = failure_kind(last_error)
                if allow_missing_tab and missing_tab_error(last_error):
                    return [], None, {
                        "attempts": attempts,
                        "duration_ms": monotonic_ms() - started,
                        "entry_count": 0,
                        "missing_tab": True,
                    }
                if kind in {
                    "content_terminal", "proxy_transport", "youtube_rate_limited",
                    "youtube_challenge", "token_or_client",
                }:
                    break
                if attempt == 0 and kind == "upstream_transient":
                    time.sleep(random.uniform(0.75, 2.25))
                else:
                    break
        return [], last_error, {
            "attempts": attempts,
            "duration_ms": monotonic_ms() - started,
            "entry_count": 0,
        }

    def channel_uploads(self, lease_id: str, channel_id: str, limit: int) -> dict[str, Any]:
        self.require_lease(lease_id)
        started = monotonic_ms()
        clean_limit = max(1, min(int(limit or 30), 100))
        playlist_id = "UU" + channel_id[2:] if channel_id.startswith("UC") else channel_id
        playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
        upload_entries, upload_error, upload_timing = self._extract_flat(playlist_url, clean_limit)
        uploads_missing = bool(upload_error and missing_uploads_error(upload_error))
        if upload_error and not uploads_missing:
            raise RuntimeError(f"uploads: {upload_error}")
        if uploads_missing:
            upload_entries = []

        stage_timings: dict[str, Any] = {"uploads": upload_timing}
        entries = []
        for position, entry in enumerate(upload_entries[:clean_limit], start=1):
            video_id = str(entry.get("id"))
            entry_url = str(entry.get("url") or entry.get("webpage_url") or "")
            live_status = str(entry.get("live_status") or "").lower()
            explicit_live = bool(entry.get("is_live")) or live_status in {
                "is_live", "is_upcoming", "upcoming",
            }
            explicit_short = "/shorts/" in entry_url
            content_type = "live" if explicit_live else "short" if explicit_short else None
            type_source = (
                "yt_dlp_uploads_live_flag"
                if explicit_live
                else "yt_dlp_uploads_url:shorts" if explicit_short else None
            )
            entries.append(
                {
                    "id": video_id,
                    "title": entry.get("title"),
                    "url": entry.get("url") or entry.get("webpage_url"),
                    "thumbnail_url": best_thumbnail(entry),
                    "duration": entry.get("duration"),
                    "view_count": entry.get("view_count"),
                    "timestamp": entry.get("timestamp"),
                    "upload_date": entry.get("upload_date"),
                    "release_timestamp": entry.get("release_timestamp"),
                    "live_status": entry.get("live_status"),
                    "is_live": entry.get("is_live"),
                    "was_live": entry.get("was_live"),
                    "position": position,
                    "content_type": content_type,
                    "type_source": type_source,
                    "type_membership": [content_type] if content_type else [],
                }
            )
        self.commands_processed += 1
        return {
            "ok": True,
            "channel_id": channel_id,
            "playlist_id": playlist_id,
            "playlist_url": playlist_url,
            "uploads_missing": uploads_missing,
            "uploads_parse_gap_count": int(upload_timing.get("parse_gap_count") or 0),
            "entries": entries,
            "tab_ids": {},
            "tab_counts": {},
            "untyped_ids": [entry["id"] for entry in entries if not entry.get("content_type")],
            "engine": "persistent",
            "stage_timings_ms": stage_timings,
            "duration_ms": monotonic_ms() - started,
        }

    def _video_detail_with_client(
        self,
        ydl: yt_dlp.YoutubeDL,
        client: str,
        url: str,
    ) -> dict[str, Any]:
        self.capture = {}
        info = self._extract(
            ydl,
            url,
            parameter_overrides=self._comment_detail_options(ydl),
        )
        if not info.get("id"):
            raise RuntimeError("yt-dlp returned no video detail")
        total_comment_count = original_comment_count(info)
        comment_info = {**info, "comment_count": total_comment_count}
        comment_count, comments_disabled, comment_status, comment_source = (
            comment_state_from_initial_data(self.capture.get("initial_data"), comment_info)
        )
        comments_first_page = normalize_ytdlp_comment_page(
            info.get("comments"),
            total_count=comment_count,
        )
        availability = availability_from_player(
            self.capture.get("player_responses"),
            info.get("availability"),
        )
        player_responses = self.capture.get("player_responses")
        playability = player_playability(player_responses)
        published_at = iso_from_timestamp(info.get("timestamp"))
        upload_date = iso_from_upload_date(info.get("upload_date"))
        if published_at:
            published_precision = "second"
        elif upload_date:
            published_precision = "date_only"
            published_at = upload_date
        else:
            published_precision = "unknown"
        live_status = str(info.get("live_status") or "")
        live_details = find_live_broadcast_details(player_responses)
        content_type_signals = player_content_type_signals(player_responses)
        live_start = next(
            (item.get("startTimestamp") for item in live_details if item.get("startTimestamp")),
            None,
        )
        live_end = next(
            (item.get("endTimestamp") for item in live_details if item.get("endTimestamp")),
            None,
        )
        release_at = iso_from_timestamp(info.get("release_timestamp"))
        is_upcoming = live_status in {"is_upcoming", "upcoming"}
        return {
            "ok": True,
            "extractor_version": "v7_persistent_ytdlp_comments",
            "client": client,
            "id": info.get("id"),
            "title": info.get("title"),
            "description": info.get("description"),
            "tags": info.get("tags"),
            "duration": info.get("duration"),
            "view_count": info.get("view_count"),
            "like_count": info.get("like_count"),
            "comment_count": comment_count,
            "comment_count_status": comment_status,
            "comments_disabled": comments_disabled,
            "comments_status_source": comment_source,
            "comments_first_page": comments_first_page,
            "comments_first_page_status": (
                "collected"
                if comments_first_page is not None
                else "disabled" if comments_disabled else "unresolved"
            ),
            "comments_first_page_source": (
                "yt_dlp_top_comments" if comments_first_page is not None else None
            ),
            "thumbnail_url": best_thumbnail(info),
            "timestamp": info.get("timestamp"),
            "upload_date": info.get("upload_date"),
            "published_at": published_at,
            "published_text": published_at[:10] if published_at else None,
            "published_at_precision": published_precision,
            "published_at_source": (
                "yt_dlp_timestamp"
                if published_precision == "second"
                else "yt_dlp_upload_date"
                if published_precision == "date_only"
                else None
            ),
            "availability": availability,
            "playability_status": playability.get("status"),
            "playability_reason": playability.get("reason"),
            "live_status": live_status,
            "is_live": info.get("is_live"),
            "was_live": info.get("was_live"),
            "live_scheduled_at": release_at if is_upcoming else None,
            "live_started_at": None if is_upcoming else live_start or release_at,
            "live_ended_at": live_end,
            "channel_id": info.get("channel_id"),
            "webpage_url": info.get("webpage_url") or info.get("url"),
            "content_type_signals": content_type_signals,
            "engine": "persistent",
        }

    def video_detail(self, lease_id: str, url: str) -> dict[str, Any]:
        self.require_lease(lease_id)
        started = monotonic_ms()
        attempts = []
        last_error = None
        clients = (("web_safari", self.primary), ("android", None))
        for client_index, (client, ydl) in enumerate(clients):
            attempt_started = monotonic_ms()
            try:
                active_ydl = ydl if ydl is not None else self._fallback_ydl()
                result = self._video_detail_with_client(active_ydl, client, url)
                attempts.append(
                    {
                        "client": client,
                        "ok": True,
                        "duration_ms": monotonic_ms() - attempt_started,
                    }
                )
                result["duration_ms"] = monotonic_ms() - started
                result["attempt_timings_ms"] = attempts
                self.commands_processed += 1
                return result
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                kind = failure_kind(last_error)
                attempts.append(
                    {
                        "client": client,
                        "ok": False,
                        "duration_ms": monotonic_ms() - attempt_started,
                        "error": last_error[:500],
                    }
                )
                if kind in {
                    "content_terminal", "proxy_transport", "youtube_rate_limited",
                    "youtube_challenge", "upstream_transient",
                }:
                    break
                if kind != "token_or_client" or client_index >= len(clients) - 1:
                    break
        raise RuntimeError(last_error or "yt-dlp returned no detail")

    def stats(self) -> dict[str, Any]:
        return {
            "pid": os.getpid(),
            "engine": "persistent",
            "active_channel": self.active_channel,
            "channels_processed": self.channels_processed,
            "commands_processed": self.commands_processed,
            "uptime_ms": round((time.time() - self.created_at) * 1000),
            "proxy_configured": bool(self.proxy),
            "profile_id": self.profile_id,
            "impersonate_target": self.impersonate_target,
            "timezone": self.timezone,
        }

    def close(self) -> None:
        for ydl in (self.primary, self.fallback):
            if ydl is None:
                continue
            close = getattr(ydl, "close", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
        self.primary = None
        self.fallback = None


def emit(payload: dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
    sys.stdout.flush()


def main() -> int:
    engine = PersistentYtDlp()
    emit(
        {
            "event": "ready",
            "pid": os.getpid(),
            "yt_dlp_version": getattr(yt_dlp.version, "__version__", "unknown"),
            "proxy_configured": bool(engine.proxy),
        }
    )
    try:
        for raw_line in sys.stdin:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            request_id = None
            try:
                request = json.loads(raw_line)
                request_id = request.get("request_id")
                command = str(request.get("command") or "")
                payload = request.get("payload") or {}
                if command == "ping":
                    result = engine.stats()
                elif command == "classify_failure":
                    result = {"kind": failure_kind(payload.get("message"))}
                elif command == "configure":
                    result = engine.configure(payload)
                elif command == "acquire":
                    result = engine.acquire(
                        str(payload["lease_id"]),
                        str(payload["channel_id"]),
                        str(payload.get("language") or engine.language),
                    )
                elif command == "release":
                    result = engine.release(str(payload["lease_id"]))
                elif command == "channel_metadata":
                    result = engine.channel_metadata(
                        str(payload["lease_id"]),
                        str(payload["url"]),
                    )
                elif command == "channel_uploads":
                    result = engine.channel_uploads(
                        str(payload["lease_id"]),
                        str(payload["channel_id"]),
                        int(payload.get("limit") or 30),
                    )
                elif command == "video_detail":
                    result = engine.video_detail(
                        str(payload["lease_id"]),
                        str(payload["url"]),
                    )
                elif command == "shutdown":
                    emit({"request_id": request_id, "ok": True, "result": engine.stats()})
                    break
                else:
                    raise ValueError(f"unknown command: {command}")
                emit({"request_id": request_id, "ok": True, "result": result})
            except Exception as exc:
                emit(
                    {
                        "request_id": request_id,
                        "ok": False,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
    finally:
        engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
