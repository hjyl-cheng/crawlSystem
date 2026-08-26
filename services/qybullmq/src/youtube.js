import { spawn } from "node:child_process";
import { ProxyAgent, fetch as undiciFetch } from "undici";
import { persistentFetch } from "./httpClient.js";
import { fetchWithFingerprint } from "./fingerprintFetch.js";
import { combineAbortSignals, throwIfAborted } from "./abortSignal.js";
import {
  currentManagedAbortSignal,
  currentProxyIdentity,
  runManagedProxyRequest,
} from "./proxyIdentity.js";
import {
  currentChannelExecutionAbortSignal,
  recordChannelExecutionFailure,
  recordChannelExecutionRequest,
} from "./channelExecutionContext.js";
import { videoAccessStatus, youtubeErrorText } from "./detailPolicy.js";
import {
  findSubscriberCountText,
  looksLikeSubscriberCountText,
  parseLocalizedCount,
  parseRequiredLocalizedCount,
} from "./localizedCount.js";
import {
  parseLocalizedAgeDays,
  parseRequiredLocalizedAgeDays,
} from "./localizedTime.js";
import { normalizeVideoKeywords, normalizeVideoTextMetadata } from "./videoMetadata.js";
import { extractYoutubePlayerContentTypeSignals } from "./youtubeContentType.js";
import {
  assertYoutubeContentObservation,
  resolveYoutubePlayability,
} from "./youtubePlayability.js";
import {
  persistentChannelMetadata,
  persistentChannelUploads,
  persistentVideoDetail,
} from "./ytdlpSession.js";
import { annotateYoutubeFailure } from "./youtubeFailurePolicy.js";
import {
  commentPageFromDataApiThreads,
  confirmedNoVisibleThreadsPage,
  emptyYoutubeCommentPage,
} from "./youtubeCommentPage.js";

const DEFAULT_LANGUAGE = process.env.YOUTUBE_LANGUAGE || "pt-BR";
const DEFAULT_COUNTRY = process.env.YOUTUBE_COUNTRY || "BR";
const DEFAULT_RECENT_DAYS = Number(process.env.YOUTUBE_RECENT_DAYS || 28);
const DEFAULT_TIMEOUT_MS = Number(process.env.YOUTUBE_FETCH_TIMEOUT_MS || 30000);
const DEFAULT_USER_AGENT = process.env.YOUTUBE_USER_AGENT
  || "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36";
const CHANNEL_FILTER_PARAM = "EgIQAg%3D%3D";
const VIDEO_OWNER_FILTER_PARAM = "CAMSAhAB";
export const VIDEO_POPULARITY_THIS_YEAR_FILTER_PARAM = "CAMSBAgFEAE=";

const YTDLP_QUICK_DETAIL_PY = String.raw`
import base64
import datetime
import json
import os
import random
import re
import string
import sys
import time
import unicodedata

import yt_dlp

from scripts.ytdlp_comments import (
    bounded_youtube_extractor_args,
    comment_limit_from_env,
    install_original_comment_count_capture,
    normalize_ytdlp_comment_page,
    original_comment_count,
)


def generate_visitor_data():
    chars = string.ascii_letters + string.digits + "_-"
    visitor_id = "".join(random.choices(chars, k=11))
    ts = int(time.time())
    id_bytes = visitor_id.encode()
    field1 = bytes([0x0A, len(id_bytes)]) + id_bytes

    def varint(n):
        out = []
        while n > 0x7F:
            out.append((n & 0x7F) | 0x80)
            n >>= 7
        out.append(n)
        return bytes(out)

    return base64.urlsafe_b64encode(field1 + bytes([0x28]) + varint(ts)).decode().rstrip("=")


def iso_from_upload_date(value):
    if isinstance(value, str) and value.isdigit() and len(value) == 8:
        return f"{value[0:4]}-{value[4:6]}-{value[6:8]}"
    return None


def iso_from_timestamp(value):
    try:
        if value is None:
            return None
        return datetime.datetime.fromtimestamp(int(value), datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def best_thumbnail(info):
    thumbs = info.get("thumbnails")
    if isinstance(thumbs, list) and thumbs:
        valid = [t for t in thumbs if isinstance(t, dict) and t.get("url")]
        if valid:
            valid.sort(key=lambda t: int(t.get("width") or 0) * int(t.get("height") or 0), reverse=True)
            return valid[0].get("url")
    return info.get("thumbnail")


def text_value(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if isinstance(value.get("simpleText"), str):
            return value["simpleText"]
        if isinstance(value.get("content"), str):
            return value["content"]
        if isinstance(value.get("runs"), list):
            return "".join(str(item.get("text") or "") for item in value["runs"] if isinstance(item, dict))
    return ""


def folded(value):
    normalized = unicodedata.normalize("NFKD", str(value or "")).lower()
    return "".join(char for char in normalized if not unicodedata.combining(char))


def parse_count(value):
    raw = text_value(value).replace("\u00a0", " ").strip().lower()
    match = re.search(r"(\d[\d.,\s]*)(?:\s*)(thousand|millions?|million|mil|mi|bilhao|bilhoes|billion|bi|[kmb])?", folded(raw))
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
            multiplier = 1_000 if unit in {"k", "mil", "thousand"} else 1_000_000 if unit in {"m", "mi", "million", "millions"} else 1_000_000_000
            return int(number * multiplier)
        digits = re.sub(r"\D", "", number_text)
        return int(digits) if digits else None
    except (TypeError, ValueError):
        return None


def comment_state_from_initial_data(initial_data, info):
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
                    disabled_words = any(token in message for token in (
                        "turned off", "disabled", "desativad", "deshabilitad", "desactivad", "desactive",
                    ))
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


def find_live_broadcast_details(value):
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


def player_content_type_signals(player_responses):
    candidates = []
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

    def merged_boolean(*paths):
        observed = []
        for candidate in candidates:
            for path in paths:
                value = candidate
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


def player_playability(player_responses):
    statuses = []
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


def capture_initial_extract_result(result):
    values = list(result) if isinstance(result, (list, tuple)) else [result]

    def is_player_response(value):
        if isinstance(value, dict):
            return any(key in value for key in ("videoDetails", "playabilityStatus", "streamingData"))
        return isinstance(value, list) and any(is_player_response(item) for item in value)

    def is_initial_data(value):
        return isinstance(value, dict) and any(key in value for key in (
            "contents", "engagementPanels", "onResponseReceivedEndpoints",
            "onResponseReceivedActions", "frameworkUpdates",
        ))

    return {
        "webpage": next((value for value in values if isinstance(value, str)), None),
        "player_responses": next((value for value in values if is_player_response(value)), None),
        "initial_data": next((value for value in values if is_initial_data(value)), None),
    }


def availability_from_player(player_responses, fallback):
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


def ydl_opts(client, language):
    proxy = os.environ.get("YOUTUBE_PROXY_URL") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    youtube_args = {
        "player_client": [client],
    }
    if client != "mweb":
        youtube_args["visitor_data"] = [generate_visitor_data()]
    youtube_args = bounded_youtube_extractor_args(youtube_args, comment_limit_from_env())
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "ignoreerrors": True,
        "getcomments": True,
        "ignore_no_formats_error": True,
        "socket_timeout": 20,
        "extractor_args": {
            "youtube": youtube_args
        },
        "http_headers": {
            "Accept-Language": f"{language},pt;q=0.9,en;q=0.8",
        },
    }
    if proxy:
        opts["proxy"] = proxy
    return opts


payload = json.load(sys.stdin)
url = payload["url"]
language = payload.get("language") or "pt-BR"
last_error = None


for client in ("web_safari", "android"):
    try:
        captured = {}
        with yt_dlp.YoutubeDL(ydl_opts(client, language)) as ydl:
            install_original_comment_count_capture(ydl)
            ie = ydl.get_info_extractor("Youtube")
            original_initial_extract = ie._initial_extract

            def capture_initial_data(*args, **kwargs):
                result = original_initial_extract(*args, **kwargs)
                captured.update(capture_initial_extract_result(result))
                return result

            ie._initial_extract = capture_initial_data
            info = ydl.extract_info(url, download=False) or {}
        if not info.get("id"):
            raise RuntimeError("yt-dlp returned no video detail")
        total_comment_count = original_comment_count(info)
        comment_info = {**info, "comment_count": total_comment_count}
        comment_count, comments_disabled, comment_count_status, comments_status_source = comment_state_from_initial_data(
            captured.get("initial_data"), comment_info
        )
        comments_first_page = normalize_ytdlp_comment_page(
            info.get("comments"), total_count=comment_count
        )
        availability = availability_from_player(captured.get("player_responses"), info.get("availability"))
        player_responses = captured.get("player_responses")
        playability = player_playability(player_responses)
        published_at = iso_from_timestamp(info.get("timestamp"))
        published_precision = "second" if published_at else "date_only" if iso_from_upload_date(info.get("upload_date")) else "unknown"
        if not published_at:
            published_at = iso_from_upload_date(info.get("upload_date"))
        live_status = str(info.get("live_status") or "")
        live_details = find_live_broadcast_details(player_responses)
        content_type_signals = player_content_type_signals(player_responses)
        live_start = next((item.get("startTimestamp") for item in live_details if item.get("startTimestamp")), None)
        live_end = next((item.get("endTimestamp") for item in live_details if item.get("endTimestamp")), None)
        release_at = iso_from_timestamp(info.get("release_timestamp"))
        is_upcoming = live_status in {"is_upcoming", "upcoming"}
        current_result = {
            "ok": True,
            "extractor_version": "v7_ytdlp_comments_first_success",
            "client": client,
            "id": info.get("id"),
            "title": info.get("title"),
            "description": info.get("description"),
            "tags": info.get("tags"),
            "duration": info.get("duration"),
            "view_count": info.get("view_count"),
            "like_count": info.get("like_count"),
            "comment_count": comment_count,
            "comment_count_status": comment_count_status,
            "comments_disabled": comments_disabled,
            "comments_status_source": comments_status_source,
            "comments_first_page": comments_first_page,
            "comments_first_page_status": "collected" if comments_first_page is not None else "disabled" if comments_disabled else "unresolved",
            "comments_first_page_source": "yt_dlp_top_comments" if comments_first_page is not None else None,
            "thumbnail_url": best_thumbnail(info),
            "timestamp": info.get("timestamp"),
            "upload_date": info.get("upload_date"),
            "published_at": published_at,
            "published_at_status": "exact" if published_at else "unresolved",
            "published_text": published_at[:10] if published_at else None,
            "published_at_precision": published_precision,
            "published_at_source": "yt_dlp_timestamp" if published_precision == "second" else "yt_dlp_upload_date" if published_precision == "date_only" else None,
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
        }
        print(json.dumps(current_result, ensure_ascii=False))
        raise SystemExit(0)
    except Exception as exc:
        last_error = f"{type(exc).__name__}: {exc}"

print(json.dumps({"ok": False, "error": last_error}, ensure_ascii=False))
raise SystemExit(2)
`;

const YTDLP_UPLOADS_PY = String.raw`
import base64
import json
import os
import random
import string
import sys
import time

import yt_dlp


def generate_visitor_data():
    chars = string.ascii_letters + string.digits + "_-"
    visitor_id = "".join(random.choices(chars, k=11))
    ts = int(time.time())
    id_bytes = visitor_id.encode()
    field1 = bytes([0x0A, len(id_bytes)]) + id_bytes

    def varint(n):
        out = []
        while n > 0x7F:
            out.append((n & 0x7F) | 0x80)
            n >>= 7
        out.append(n)
        return bytes(out)

    return base64.urlsafe_b64encode(field1 + bytes([0x28]) + varint(ts)).decode().rstrip("=")


def best_thumbnail(info):
    thumbs = info.get("thumbnails")
    if isinstance(thumbs, list) and thumbs:
        valid = [t for t in thumbs if isinstance(t, dict) and t.get("url")]
        if valid:
            valid.sort(key=lambda t: int(t.get("width") or 0) * int(t.get("height") or 0), reverse=True)
            return valid[0].get("url")
    return info.get("thumbnail")


def ydl_opts(language, limit):
    proxy = os.environ.get("YOUTUBE_PROXY_URL") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": "in_playlist",
        "playlistend": limit,
        "ignoreerrors": False,
        "socket_timeout": 25,
        "extractor_args": {
            "youtube": {
                "player_client": ["web_safari"],
                "player_skip": ["js", "configs"],
                "visitor_data": [generate_visitor_data()],
            }
        },
        "http_headers": {"Accept-Language": f"{language},pt;q=0.9,en;q=0.8"},
    }
    if proxy:
        opts["proxy"] = proxy
    return opts


def missing_tab_error(message):
    lowered = str(message).lower()
    return "does not have a" in lowered and "tab" in lowered


def missing_uploads_error(message):
    lowered = str(message).lower()
    return "http error 404" in lowered or "playlist does not exist" in lowered


def extract_flat(url, language, limit, allow_missing_tab=False):
    last_error = None
    for attempt in range(2):
        try:
            with yt_dlp.YoutubeDL(ydl_opts(language, limit)) as ydl:
                info = ydl.extract_info(url, download=False) or {}
            source_entries = info.get("entries") or []
            entries = [entry for entry in source_entries if isinstance(entry, dict) and entry.get("id")]
            return entries, None, len(source_entries) - len(entries)
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if allow_missing_tab and missing_tab_error(last_error):
                return [], None, 0
            if attempt == 0:
                time.sleep(1.5)
    return [], last_error, 0


payload = json.load(sys.stdin)
channel_id = str(payload["channel_id"])
limit = max(1, min(int(payload.get("limit") or 30), 100))
language = payload.get("language") or "pt-BR"
playlist_id = "UU" + channel_id[2:] if channel_id.startswith("UC") else channel_id
playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
upload_entries, upload_error, uploads_parse_gap_count = extract_flat(playlist_url, language, limit)
uploads_missing = bool(upload_error and missing_uploads_error(upload_error))
if upload_error and not uploads_missing:
    print(json.dumps({"ok": False, "error": f"uploads: {upload_error}"}, ensure_ascii=False))
    raise SystemExit(0)
if uploads_missing:
    upload_entries = []

entries = []
for position, entry in enumerate(upload_entries[:limit], start=1):
    video_id = str(entry.get("id"))
    entry_url = str(entry.get("url") or entry.get("webpage_url") or "")
    live_status = str(entry.get("live_status") or "").lower()
    explicit_live = bool(entry.get("is_live")) or live_status in ("is_live", "is_upcoming", "upcoming")
    explicit_short = "/shorts/" in entry_url
    content_type = "live" if explicit_live else "short" if explicit_short else None
    type_source = "yt_dlp_uploads_live_flag" if explicit_live else "yt_dlp_uploads_url:shorts" if explicit_short else None
    entries.append({
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
    })

print(json.dumps({
    "ok": True,
    "channel_id": channel_id,
    "playlist_id": playlist_id,
    "playlist_url": playlist_url,
    "uploads_missing": uploads_missing,
    "uploads_parse_gap_count": uploads_parse_gap_count,
    "entries": entries,
    "tab_ids": {},
    "tab_counts": {},
    "untyped_ids": [entry["id"] for entry in entries if not entry.get("content_type")],
}, ensure_ascii=False))
`;

const YTDLP_CHANNEL_METADATA_PY = String.raw`
import base64
import json
import os
import random
import string
import sys
import time

import yt_dlp


def generate_visitor_data():
    chars = string.ascii_letters + string.digits + "_-"
    visitor_id = "".join(random.choices(chars, k=11))
    ts = int(time.time())
    id_bytes = visitor_id.encode()
    field1 = bytes([0x0A, len(id_bytes)]) + id_bytes

    def varint(n):
        out = []
        while n > 0x7F:
            out.append((n & 0x7F) | 0x80)
            n >>= 7
        out.append(n)
        return bytes(out)

    return base64.urlsafe_b64encode(field1 + bytes([0x28]) + varint(ts)).decode().rstrip("=")


def best_avatar(info):
    thumbs = info.get("thumbnails")
    if not isinstance(thumbs, list):
        return info.get("thumbnail")
    valid = [t for t in thumbs if isinstance(t, dict) and t.get("url")]
    avatars = [
        t for t in valid
        if "avatar" in str(t.get("id", "")).lower()
        or (int(t.get("width") or 0) > 0 and int(t.get("width") or 0) == int(t.get("height") or 0))
    ]
    pool = avatars or valid
    if not pool:
        return info.get("thumbnail")
    pool.sort(key=lambda t: int(t.get("width") or 0) * int(t.get("height") or 0), reverse=True)
    return pool[0].get("url")


def ydl_opts(language):
    proxy = os.environ.get("YOUTUBE_PROXY_URL") or os.environ.get("HTTPS_PROXY") or os.environ.get("HTTP_PROXY")
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "playlistend": 1,
        "ignoreerrors": True,
        "socket_timeout": 25,
        "extractor_args": {
            "youtube": {
                "player_client": ["web_safari"],
                "player_skip": ["js", "configs"],
                "visitor_data": [generate_visitor_data()],
            }
        },
        "http_headers": {
            "Accept-Language": f"{language},pt;q=0.9,en;q=0.8",
        },
    }
    if proxy:
        opts["proxy"] = proxy
    return opts


payload = json.load(sys.stdin)
url = payload["url"]
language = payload.get("language") or "pt-BR"
with yt_dlp.YoutubeDL(ydl_opts(language)) as ydl:
    info = ydl.extract_info(url, download=False) or {}
print(json.dumps({
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
}, ensure_ascii=False))
`;

let proxyAgent = null;
let proxyAgentUrl = null;

export async function closeYoutubeHttpProxyAgent() {
  const current = proxyAgent;
  proxyAgent = null;
  proxyAgentUrl = null;
  if (!current) return;
  try {
    await current.close();
  } catch {
    current.destroy();
  }
}

function youtubeProxyUrl() {
  const raw = String(currentProxyIdentity().proxy_url || process.env.YOUTUBE_PROXY_URL || "").trim();
  return raw || null;
}

function dispatcher() {
  const activeDispatcher = currentProxyIdentity().dispatcher;
  if (activeDispatcher) return activeDispatcher;
  const proxyUrl = youtubeProxyUrl();
  if (!proxyUrl) return undefined;
  if (!proxyAgent || proxyAgentUrl !== proxyUrl) {
    const stale = proxyAgent;
    proxyAgent = new ProxyAgent(proxyUrl);
    proxyAgentUrl = proxyUrl;
    if (stale) {
      const timer = setTimeout(() => void stale.close().catch(() => stale.destroy()), 30000);
      timer.unref?.();
    }
  }
  return proxyAgent;
}

function proxyEnv() {
  const proxyUrl = youtubeProxyUrl();
  if (!proxyUrl) return {};
  return {
    YOUTUBE_PROXY_URL: proxyUrl,
    HTTP_PROXY: proxyUrl,
    HTTPS_PROXY: proxyUrl,
    http_proxy: proxyUrl,
    https_proxy: proxyUrl,
  };
}

function youtubeBlockSignal(text) {
  const body = String(text ?? "");
  if (isUsableYoutubePage(body)) return false;
  return /sorry\/index|detected unusual traffic|automated queries|to continue[^<]{0,120}(characters|captcha)|sign in to confirm[^<]{0,120}not a bot|confirm[^<]{0,120}not a bot|verify you are human|login required/i
    .test(body);
}

function youtubeRequestError(message, {
  status = null,
  body = "",
  source = "youtube",
  targetUrl = null,
  client = "WEB",
} = {}) {
  return annotateYoutubeFailure(new Error(message), {
    status,
    body,
    source,
    targetUrl,
    client,
  });
}

function annotateYtDlpFailure(error, targetUrl, source) {
  const annotated = annotateYoutubeFailure(error, { source, targetUrl, client: "web_safari" });
  recordChannelExecutionFailure({
    error: annotated,
    engine: "yt_dlp",
    client: "web_safari",
    source,
    targetUrl,
  });
  return annotated;
}

function isUsableYoutubePage(text) {
  const body = String(text ?? "");
  return /ytInitialData|ytInitialPlayerResponse/.test(body)
    && /videoRenderer|channelRenderer|reelShelfRenderer|gridVideoRenderer|itemSectionRenderer|twoColumnSearchResultsRenderer|videoDetails|playerMicroformatRenderer/.test(body);
}

function runPythonJson(script, payload, {
  timeoutMs = 90000,
  maxBuffer = 20 * 1024 * 1024,
  signal = null,
} = {}) {
  throwIfAborted(signal);
  return new Promise((resolve, reject) => {
    const child = spawn("python3", ["-c", script], {
      stdio: ["pipe", "pipe", "pipe"],
      env: {
        ...process.env,
        ...proxyEnv(),
      },
    });
    let stdout = "";
    let stderr = "";
    let settled = false;
    let timer = null;
    const cleanup = () => {
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
    };
    const finish = (callback, value) => {
      if (settled) return;
      settled = true;
      cleanup();
      callback(value);
    };
    const terminate = () => {
      if (child.killed) return;
      try {
        child.kill("SIGKILL");
      } catch {
        // The helper is already gone.
      }
    };
    const onAbort = () => {
      finish(reject, signal.reason);
      terminate();
    };
    timer = setTimeout(() => {
      const error = new Error(`python helper timeout ${timeoutMs}ms`);
      finish(reject, error);
      terminate();
    }, timeoutMs);
    signal?.addEventListener("abort", onAbort, { once: true });
    if (signal?.aborted) {
      onAbort();
      return;
    }
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
      if (stdout.length > maxBuffer) {
        finish(reject, new Error("python helper stdout exceeded max buffer"));
        terminate();
      }
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
      if (stderr.length > maxBuffer) {
        finish(reject, new Error("python helper stderr exceeded max buffer"));
        terminate();
      }
    });
    child.on("error", (error) => {
      finish(reject, error);
    });
    child.on("close", (code) => {
      if (code !== 0) {
        finish(reject, new Error(String(stderr || stdout || `python helper exited ${code}`).trim()));
        return;
      }
      try {
        finish(resolve, JSON.parse(String(stdout || "{}")));
      } catch (error) {
        finish(reject, new Error(`python helper returned invalid JSON: ${error?.message || error}`));
      }
    });
    try {
      child.stdin.end(JSON.stringify(payload ?? {}));
    } catch (error) {
      finish(reject, error);
    }
  });
}

function youtubeAdapterAbortSignal(signal) {
  return combineAbortSignals(
    signal,
    currentManagedAbortSignal(),
    currentChannelExecutionAbortSignal(),
  );
}

function defaultHeaders(language = DEFAULT_LANGUAGE) {
  return {
    "user-agent": DEFAULT_USER_AGENT,
    "accept-language": `${language},pt;q=0.9,en;q=0.8`,
  };
}

export function absoluteYoutubeUrl(url) {
  if (!url) return null;
  if (url.startsWith("http://") || url.startsWith("https://")) return url;
  if (url.startsWith("//")) return `https:${url}`;
  if (url.startsWith("/")) return `https://www.youtube.com${url}`;
  return `https://www.youtube.com/${url}`;
}

function channelBaseUrl(channelUrl) {
  const url = new URL(absoluteYoutubeUrl(channelUrl));
  url.hash = "";
  url.search = "";
  url.pathname = url.pathname.replace(/\/+$/, "");
  return url;
}

function latestTabParams(tab) {
  if (tab === "streams" || tab === "lives") return { view: "2", sort: "dd", shelf_id: "0" };
  if (tab === "videos" || tab === "shorts") return { view: "0", sort: "dd", shelf_id: "0" };
  return {};
}

export function textValue(value) {
  if (!value) return null;
  if (typeof value === "string") return value.trim() || null;
  if (typeof value.content === "string") return value.content.trim() || null;
  if (typeof value.simpleText === "string") return value.simpleText.trim() || null;
  if (Array.isArray(value.runs)) {
    const text = value.runs.map((run) => run?.text ?? "").join("").trim();
    return text || null;
  }
  const label = value.accessibility?.accessibilityData?.label ?? value.accessibilityLabel;
  return typeof label === "string" && label.trim() ? label.trim() : null;
}

export function bestThumbnail(value) {
  const list = value?.thumbnails ?? value?.sources ?? value?.image?.sources ?? value;
  if (!Array.isArray(list) || list.length === 0) return null;
  const sorted = [...list].sort((a, b) => Number(b.width ?? 0) - Number(a.width ?? 0));
  const url = sorted[0]?.url;
  return typeof url === "string" ? absoluteYoutubeUrl(url) : null;
}

function stripAccents(value) {
  return String(value ?? "").normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

export function parseCountText(text, locale = DEFAULT_LANGUAGE) {
  return parseLocalizedCount(text, { locale });
}

function extractBalancedObjectAt(html, start) {
  if (start < 0 || html[start] !== "{") return null;
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = start; i < html.length; i += 1) {
    const ch = html[i];
    if (inString) {
      if (escaped) escaped = false;
      else if (ch === "\\") escaped = true;
      else if (ch === "\"") inString = false;
      continue;
    }
    if (ch === "\"") inString = true;
    else if (ch === "{") depth += 1;
    else if (ch === "}") {
      depth -= 1;
      if (depth === 0) return html.slice(start, i + 1);
    }
  }
  return null;
}

function extractBalancedObject(html, marker) {
  const markerIndex = html.indexOf(marker);
  if (markerIndex < 0) return null;
  const start = html.indexOf("{", markerIndex + marker.length);
  return extractBalancedObjectAt(html, start);
}

function mergeDefined(base, patch) {
  const out = { ...(base ?? {}) };
  for (const [key, value] of Object.entries(patch ?? {})) {
    if (value !== undefined && value !== null && value !== "") out[key] = value;
  }
  return out;
}

function optionalInteger(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

function optionalPositiveInteger(value) {
  const number = optionalInteger(value);
  return number != null && number > 0 ? number : null;
}

function jsonStringField(html, field) {
  const pattern = new RegExp(`"${field}"\\s*:\\s*"([^"]+)"`);
  const match = html.match(pattern);
  return match?.[1]?.replace(/\\u0026/g, "&") ?? null;
}

function isoToTimestamp(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const date = /^\d{4}-\d{2}-\d{2}$/.test(text) ? new Date(`${text}T00:00:00.000Z`) : new Date(text);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

function dateTextFromIsoLike(value) {
  const match = String(value ?? "").trim().match(/^(\d{4}-\d{2}-\d{2})/);
  return match?.[1] ?? null;
}

export function durationTextFromSeconds(value) {
  const seconds = Number(value);
  if (!Number.isFinite(seconds) || seconds <= 0) return null;
  const total = Math.floor(seconds);
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return h > 0
    ? `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`
    : `${m}:${String(s).padStart(2, "0")}`;
}

function secondsFromIsoDuration(value) {
  const text = String(value ?? "").trim();
  if (!text) return null;
  const match = text.match(/^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$/i);
  if (!match) return null;
  const days = Number(match[1] ?? 0);
  const hours = Number(match[2] ?? 0);
  const minutes = Number(match[3] ?? 0);
  const seconds = Number(match[4] ?? 0);
  const total = days * 86400 + hours * 3600 + minutes * 60 + seconds;
  return Number.isFinite(total) && total > 0 ? total : null;
}

export function durationTextFromIsoDuration(value) {
  return durationTextFromSeconds(secondsFromIsoDuration(value));
}

export function extractYtInitialPlayerResponse(html) {
  for (const marker of ["var ytInitialPlayerResponse =", "window[\"ytInitialPlayerResponse\"] =", "ytInitialPlayerResponse ="]) {
    const json = extractBalancedObject(html, marker);
    if (json) return JSON.parse(json);
  }
  return null;
}

export function extractYtInitialData(html) {
  for (const marker of ["var ytInitialData =", "window[\"ytInitialData\"] =", "ytInitialData ="]) {
    const json = extractBalancedObject(html, marker);
    if (json) return JSON.parse(json);
  }
  throw new Error("ytInitialData not found");
}

export function extractYtConfig(html) {
  let cfg = {};
  let searchFrom = 0;
  while (true) {
    const markerIndex = html.indexOf("ytcfg.set({", searchFrom);
    if (markerIndex < 0) break;
    const start = html.indexOf("{", markerIndex);
    const json = extractBalancedObjectAt(html, start);
    searchFrom = markerIndex + 1;
    if (!json) continue;
    try {
      const parsed = JSON.parse(json);
      if (parsed?.INNERTUBE_API_KEY || parsed?.INNERTUBE_CONTEXT) cfg = { ...cfg, ...parsed };
    } catch {
      continue;
    }
  }
  const apiKey = cfg.INNERTUBE_API_KEY ?? html.match(/"INNERTUBE_API_KEY":"([^"]+)"/)?.[1] ?? null;
  const clientVersion = cfg.INNERTUBE_CONTEXT_CLIENT_VERSION
    ?? cfg.INNERTUBE_CLIENT_VERSION
    ?? cfg.INNERTUBE_CONTEXT?.client?.clientVersion
    ?? html.match(/"INNERTUBE_CONTEXT_CLIENT_VERSION":"([^"]+)"/)?.[1]
    ?? "2.20260706.00.00";
  return {
    apiKey,
    context: cfg.INNERTUBE_CONTEXT ?? {
      client: {
        clientName: "WEB",
        clientVersion,
        hl: DEFAULT_LANGUAGE,
        gl: DEFAULT_COUNTRY,
      },
    },
    clientName: cfg.INNERTUBE_CONTEXT_CLIENT_NAME ?? "1",
    clientVersion,
    visitorData: cfg.VISITOR_DATA ?? cfg.INNERTUBE_CONTEXT?.client?.visitorData ?? null,
  };
}

export function findAll(root, key) {
  const out = [];
  const walk = (value) => {
    if (!value || typeof value !== "object") return;
    if (value[key]) out.push(value[key]);
    if (Array.isArray(value)) {
      for (const item of value) walk(item);
      return;
    }
    for (const item of Object.values(value)) walk(item);
  };
  walk(root);
  return out;
}

export function findFirst(root, key) {
  let found = null;
  const walk = (value) => {
    if (found || !value || typeof value !== "object") return;
    if (value[key]) {
      found = value[key];
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) walk(item);
      return;
    }
    for (const item of Object.values(value)) walk(item);
  };
  walk(root);
  return found;
}

export function findContinuationToken(root) {
  let token = null;
  const walk = (value) => {
    if (token || !value || typeof value !== "object") return;
    const endpoint = value.continuationItemRenderer?.continuationEndpoint;
    const candidate = endpoint?.continuationCommand?.token
      ?? endpoint?.command?.continuationCommand?.token
      ?? value.continuationItemRenderer?.button?.buttonRenderer?.command?.continuationCommand?.token;
    if (typeof candidate === "string" && candidate.trim()) {
      token = candidate;
      return;
    }
    if (Array.isArray(value)) {
      for (const item of value) walk(item);
      return;
    }
    for (const item of Object.values(value)) walk(item);
  };
  walk(root);
  return token;
}

export async function youtubeFetch(url, init = {}) {
  const timeoutMs = Number(init.timeoutMs ?? DEFAULT_TIMEOUT_MS);
  const startedAt = Date.now();
  const externalSignal = combineAbortSignals(init.signal, currentManagedAbortSignal());
  throwIfAborted(externalSignal);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`timeout ${timeoutMs}ms`)), timeoutMs);
  const requestDispatcher = dispatcher();
  try {
    const requestInit = {
      ...init,
      headers: {
        ...defaultHeaders(init.language || DEFAULT_LANGUAGE),
        ...(init.headers ?? {}),
      },
      signal: combineAbortSignals(externalSignal, controller.signal),
    };
    const response = await runManagedProxyRequest(() => fetchWithFingerprint(
      "youtubejs_chrome",
      url,
      requestInit,
      () => undiciFetch(url, { ...requestInit, dispatcher: requestDispatcher }),
    ));
    throwIfAborted(externalSignal);
    recordChannelExecutionRequest({
      engine: "youtube_http",
      client: "WEB",
      status: response.status,
      durationMs: Date.now() - startedAt,
      ok: response.ok,
      source: "youtube_fetch",
      targetUrl: String(url),
    });
    return response;
  } catch (error) {
    if (externalSignal?.aborted) throw externalSignal.reason;
    const annotated = annotateYoutubeFailure(error, {
      source: "youtube_fetch_transport",
      targetUrl: String(url),
      client: "WEB",
    });
    recordChannelExecutionRequest({
      engine: "youtube_http",
      client: "WEB",
      durationMs: Date.now() - startedAt,
      ok: false,
      error: annotated,
      source: "youtube_fetch_transport",
      targetUrl: String(url),
    });
    throw annotated;
  } finally {
    clearTimeout(timer);
  }
}

export async function youtubeText(url, init = {}) {
  const response = await youtubeFetch(url, init);
  const text = await response.text();
  if (youtubeBlockSignal(text)) {
    const error = youtubeRequestError(`YouTube bot challenge HTTP ${response.status}`, {
      status: response.status,
      body: text.slice(0, 500),
      source: "youtube_text",
      targetUrl: String(url),
    });
    recordChannelExecutionFailure({
      error,
      engine: "youtube_http",
      client: "WEB",
      status: response.status,
      body: text,
      source: "youtube_text",
      targetUrl: String(url),
    });
    throw error;
  }
  if (!response.ok) {
    throw youtubeRequestError(`youtube fetch failed ${response.status}: ${text.slice(0, 240)}`, {
      status: response.status,
      body: text,
      source: "youtube_text",
      targetUrl: String(url),
    });
  }
  return { text, status: response.status, url };
}

export async function detectProxyIp() {
  const response = await youtubeFetch("https://api.ipify.org?format=json", { timeoutMs: 10000 });
  const text = await response.text();
  if (!response.ok) throw new Error(`ipify failed ${response.status}: ${text.slice(0, 120)}`);
  try {
    const parsed = JSON.parse(text);
    return parsed?.ip ? String(parsed.ip) : null;
  } catch {
    return text.trim() || null;
  }
}

export function classifyYoutubeError(error, httpStatus = null) {
  const text = youtubeErrorText(error).toLowerCase();
  if (/sign in|not a bot|confirm.*bot|captcha|login required/.test(text)) return "ip_blocked_or_rate_limited";
  if (httpStatus === 403 || httpStatus === 429) return "ip_blocked_or_rate_limited";
  if (httpStatus && httpStatus >= 500) return "upstream_5xx";
  if (/timeout|timed out|abort/.test(text)) return "timeout";
  if (/econnreset|econnrefused|unable to connect|connection refused|socket|network|fetch failed|dns|enotfound|eai_again/.test(text)) return "network";
  if (/ytinitialdata not found|parse|json/.test(text)) return "parse";
  return "unknown_error";
}

export function buildYoutubeSearchUrl(
  query,
  language = DEFAULT_LANGUAGE,
  country = DEFAULT_COUNTRY,
  filterParam = CHANNEL_FILTER_PARAM,
) {
  const url = new URL("https://www.youtube.com/results");
  url.searchParams.set("search_query", query);
  if (filterParam) url.searchParams.set("sp", filterParam);
  url.searchParams.set("hl", language);
  url.searchParams.set("gl", country);
  return url.toString();
}

export async function fetchSearchInitial(query, { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY, filterParam = CHANNEL_FILTER_PARAM } = {}) {
  const url = buildYoutubeSearchUrl(query, language, country, filterParam);
  const { text } = await youtubeText(url, { language });
  return {
    url,
    rawText: text,
    rawContentType: "text/html; charset=utf-8",
    initialData: extractYtInitialData(text),
    ytConfig: extractYtConfig(text),
  };
}

export async function fetchVideoOwnerSearchInitial(query, { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY } = {}) {
  return fetchSearchInitial(query, { language, country, filterParam: VIDEO_OWNER_FILTER_PARAM });
}

export async function fetchPopularThisYearVideoSearchInitial(
  query,
  { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY } = {},
) {
  return fetchSearchInitial(query, {
    language,
    country,
    filterParam: VIDEO_POPULARITY_THIS_YEAR_FILTER_PARAM,
  });
}

export async function fetchSearchContinuation(ytConfig, continuation, { language = DEFAULT_LANGUAGE } = {}) {
  if (!ytConfig?.apiKey) throw new Error("youtube innertube api key missing");
  const response = await youtubeFetch(`https://www.youtube.com/youtubei/v1/search?key=${encodeURIComponent(ytConfig.apiKey)}`, {
    method: "POST",
    language,
    headers: {
      "content-type": "application/json",
      "x-youtube-client-name": String(ytConfig.clientName ?? "1"),
      "x-youtube-client-version": String(ytConfig.clientVersion ?? "2.20260706.00.00"),
    },
    body: JSON.stringify({
      context: ytConfig.context,
      continuation,
    }),
  });
  const text = await response.text();
  if (!response.ok) throw youtubeRequestError(`youtube search continuation failed ${response.status}: ${text.slice(0, 240)}`, {
    status: response.status,
    body: text,
    source: "youtube_search_continuation",
    targetUrl: response.url,
  });
  return { rawText: text, rawContentType: "application/json; charset=utf-8", initialData: JSON.parse(text), ytConfig };
}

export function extractChannelCandidates(root, queryText = "", queryId = null, locale = DEFAULT_LANGUAGE) {
  return findAll(root, "channelRenderer")
    .map((renderer, index) => channelRendererToCandidate(renderer, queryText, queryId, index + 1, locale))
    .filter(Boolean);
}

export function extractVideoOwnerCandidates(root, queryText = "", queryId = null, locale = DEFAULT_LANGUAGE) {
  const videos = findAll(root, "videoRenderer");
  return videos
    .map((renderer, index) => videoRendererToOwnerCandidate(renderer, queryText, queryId, index + 1, locale))
    .filter(Boolean);
}

function channelRendererToCandidate(renderer, queryText, queryId, rank, locale) {
  const channelId = renderer.channelId ?? renderer.navigationEndpoint?.browseEndpoint?.browseId ?? null;
  if (!channelId) return null;
  const canonical = renderer.navigationEndpoint?.browseEndpoint?.canonicalBaseUrl ?? null;
  const channelUrl = absoluteYoutubeUrl(canonical) ?? `https://www.youtube.com/channel/${channelId}`;
  const subscriberOrHandle = textValue(renderer.subscriberCountText);
  const handle = renderer.handleText?.simpleText
    ?? (canonical?.startsWith("/@") ? canonical.slice(1) : null)
    ?? (subscriberOrHandle?.startsWith("@") ? subscriberOrHandle : null);
  const secondaryCountText = textValue(renderer.videoCountText);
  const subscriberCountText = subscriberOrHandle && !subscriberOrHandle.startsWith("@")
    ? subscriberOrHandle
    : looksLikeSubscriberCountText(secondaryCountText)
      ? secondaryCountText
      : null;
  const subscriberCount = subscriberCountText == null
    ? null
    : parseRequiredLocalizedCount(subscriberCountText, {
        locale,
        field: "subscriber_count",
        source: "youtube_search_channel_renderer",
        context: { channel_id: channelId, query: queryText },
      });
  return {
    channel_id: channelId,
    entity_key: `youtube:channel:${channelId}`,
    title: textValue(renderer.title),
    handle,
    channel_url: channelUrl,
    description: textValue(renderer.descriptionSnippet),
    avatar_url: bestThumbnail(renderer.thumbnail),
    subscriber_count_text: subscriberCountText,
    subscriber_count: subscriberCount,
    is_verified: Boolean(renderer.ownerBadges?.length),
    rank_position: rank,
    query_id: queryId,
    query_text: queryText,
    raw: renderer,
  };
}

function videoRendererToOwnerCandidate(renderer, queryText, queryId, rank, locale) {
  const videoId = renderer.videoId;
  if (!videoId) return null;
  const ownerEndpoint = renderer.ownerText?.runs?.[0]?.navigationEndpoint?.browseEndpoint
    ?? renderer.longBylineText?.runs?.[0]?.navigationEndpoint?.browseEndpoint
    ?? renderer.shortBylineText?.runs?.[0]?.navigationEndpoint?.browseEndpoint
    ?? null;
  const channelId = ownerEndpoint?.browseId ?? null;
  if (!channelId || !String(channelId).startsWith("UC")) return null;
  const canonical = ownerEndpoint?.canonicalBaseUrl ?? null;
  const channelUrl = absoluteYoutubeUrl(canonical) ?? `https://www.youtube.com/channel/${channelId}`;
  const ownerName = textValue(renderer.ownerText) ?? textValue(renderer.longBylineText) ?? textValue(renderer.shortBylineText);
  const publishedText = textValue(renderer.publishedTimeText);
  const viewCountText = textValue(renderer.viewCountText) ?? textValue(renderer.shortViewCountText);
  const viewCount = viewCountText == null
    ? null
    : parseRequiredLocalizedCount(viewCountText, {
        locale,
        field: "view_count",
        source: "youtube_search_video_renderer",
        context: { video_id: videoId, channel_id: channelId, query: queryText },
      });
  return {
    channel_id: channelId,
    entity_key: `youtube:channel:${channelId}`,
    title: ownerName,
    handle: canonical?.startsWith("/@") ? canonical.slice(1) : null,
    channel_url: channelUrl,
    description: null,
    avatar_url: bestThumbnail(renderer.channelThumbnailSupportedRenderers?.channelThumbnailWithLinkRenderer?.thumbnail),
    subscriber_count_text: null,
    subscriber_count: null,
    is_verified: null,
    rank_position: rank,
    query_id: queryId,
    query_text: queryText,
    discovery_strategy: "video_owners",
    score: 50 + Math.max(0, 30 - rank),
    source_video: {
      video_id: videoId,
      title: textValue(renderer.title),
      url: absoluteYoutubeUrl(renderer.navigationEndpoint?.commandMetadata?.webCommandMetadata?.url ?? `/watch?v=${videoId}`),
      view_count_text: viewCountText,
      view_count: viewCount,
      published_text: publishedText,
      thumbnail_url: bestThumbnail(renderer.thumbnail),
    },
    raw: renderer,
  };
}

function metadataParts(header) {
  const parts = header?.content?.pageHeaderViewModel?.metadata?.contentMetadataViewModel
    ?.metadataRows?.flatMap((row) => row?.metadataParts ?? []) ?? [];
  return parts.map((part) => textValue(part.text) ?? part.accessibilityLabel ?? null).filter(Boolean);
}

export function parseChannelHeader(root, locale = DEFAULT_LANGUAGE) {
  const metadata = findFirst(root, "channelMetadataRenderer");
  const pageHeader = findFirst(root, "pageHeaderRenderer");
  const c4Header = findFirst(root, "c4TabbedHeaderRenderer");
  const microformat = findFirst(root, "microformatDataRenderer");
  const about = findFirst(root, "aboutChannelRenderer")?.metadata?.aboutChannelViewModel;
  const viewModel = pageHeader?.content?.pageHeaderViewModel;
  const parts = metadataParts(pageHeader);
  const title = metadata?.title ?? textValue(viewModel?.title?.dynamicTextViewModel?.text) ?? microformat?.title ?? about?.title ?? null;
  const channelId = metadata?.externalId ?? about?.channelId ?? null;
  const vanity = metadata?.ownerUrls?.[0] ?? metadata?.vanityChannelUrl ?? about?.canonicalChannelUrl ?? null;
  const handle = parts.find((part) => String(part).startsWith("@"))
    ?? (typeof vanity === "string" && vanity.includes("/@") ? `@${vanity.split("/@")[1]}` : null);
  const aboutSubscriberCountText = textValue(about?.subscriberCountText);
  const headerSubscriberCountText = findSubscriberCountText(parts, { locale });
  const subscriberCountText = aboutSubscriberCountText ?? headerSubscriberCountText ?? null;
  const aboutMetadataLoaded = Boolean(about);
  const attributedTitle = viewModel?.title?.dynamicTextViewModel?.text;
  const pageHeaderVerificationObserved = Boolean(attributedTitle && typeof attributedTitle === "object");
  const c4VerificationObserved = Array.isArray(c4Header?.badges);
  const verified = Boolean(
    attributedTitle?.attachmentRuns?.length
    || /verificado|verified/i.test(String(
      viewModel?.title?.dynamicTextViewModel?.rendererContext?.accessibilityContext?.label ?? "",
    ))
    || c4Header?.badges?.some((badge) => (
      /VERIFIED/i.test(String(badge?.metadataBadgeRenderer?.style ?? ""))
      || /verificado|verified/i.test(String(badge?.metadataBadgeRenderer?.label ?? ""))
    )),
  );
  const verificationObserved = pageHeaderVerificationObserved || c4VerificationObserved;
  const subscriberCount = subscriberCountText == null && aboutMetadataLoaded
    ? 0
    : subscriberCountText == null
      ? null
      : parseRequiredLocalizedCount(subscriberCountText, {
          locale,
          field: "subscriber_count",
          source: aboutSubscriberCountText ? "youtube_about" : "youtube_channel_header",
          context: { channel_id: channelId },
        });
  return {
    channel_id: channelId,
    title,
    handle,
    channel_url: metadata?.channelUrl ?? microformat?.urlCanonical ?? (channelId ? `https://www.youtube.com/channel/${channelId}` : null),
    vanity_channel_url: vanity,
    description: about?.description ?? metadata?.description ?? microformat?.description ?? null,
    avatar_url: bestThumbnail(metadata?.avatar)
      ?? bestThumbnail(viewModel?.image?.decoratedAvatarViewModel?.avatar?.avatarViewModel?.image)
      ?? bestThumbnail(microformat?.thumbnail),
    subscriber_count_text: subscriberCountText ?? (subscriberCount === 0 ? "0 subscribers" : null),
    subscriber_count: subscriberCount,
    subscriber_count_source: subscriberCountText
      ? (aboutSubscriberCountText ? "youtube_about" : "youtube_channel_header")
      : subscriberCount === 0
        ? "youtube_about_missing_subscriber_row"
        : null,
    video_count_text: about?.videoCountText ?? parts.find((part) => /video|video/i.test(stripAccents(part))) ?? null,
    view_count_text: about?.viewCountText ?? null,
    joined_date_text: textValue(about?.joinedDateText),
    country: about?.country ?? null,
    is_verified: verificationObserved ? verified : null,
    is_verified_status: verificationObserved
      ? verified ? "verified" : "not_verified"
      : "unknown",
  };
}

export async function fetchChannelInitial(channelUrl, { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY } = {}) {
  const target = channelBaseUrl(channelUrl);
  target.searchParams.set("hl", language);
  target.searchParams.set("gl", country);
  const url = target.toString();
  const { text } = await youtubeText(url, { language });
  return {
    url,
    rawText: text,
    rawContentType: "text/html; charset=utf-8",
    initialData: extractYtInitialData(text),
    ytConfig: extractYtConfig(text),
  };
}

export async function fetchChannelTabInitial(channelUrl, tab, { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY } = {}) {
  const target = channelBaseUrl(channelUrl);
  target.pathname = `${target.pathname}/${tab}`;
  for (const [key, value] of Object.entries(latestTabParams(tab))) {
    target.searchParams.set(key, value);
  }
  target.searchParams.set("hl", language);
  target.searchParams.set("gl", country);
  const url = target.toString();
  const { text } = await youtubeText(url, { language });
  return {
    url,
    rawText: text,
    rawContentType: "text/html; charset=utf-8",
    initialData: extractYtInitialData(text),
    ytConfig: extractYtConfig(text),
  };
}

export async function fetchBrowseContinuation(ytConfig, continuation, { language = DEFAULT_LANGUAGE } = {}) {
  if (!ytConfig?.apiKey) throw new Error("youtube innertube api key missing");
  const response = await youtubeFetch(`https://www.youtube.com/youtubei/v1/browse?key=${encodeURIComponent(ytConfig.apiKey)}`, {
    method: "POST",
    language,
    headers: {
      "content-type": "application/json",
      "x-youtube-client-name": String(ytConfig.clientName ?? "1"),
      "x-youtube-client-version": String(ytConfig.clientVersion ?? "2.20260706.00.00"),
    },
    body: JSON.stringify({
      context: ytConfig.context,
      continuation,
    }),
  });
  const text = await response.text();
  if (!response.ok) throw youtubeRequestError(`youtube browse continuation failed ${response.status}: ${text.slice(0, 240)}`, {
    status: response.status,
    body: text,
    source: "youtube_browse_continuation",
    targetUrl: response.url,
  });
  return { rawText: text, rawContentType: "application/json; charset=utf-8", initialData: JSON.parse(text), ytConfig };
}

function detailFromPlayerResponse(player, url = null) {
  if (!player || typeof player !== "object") return {};
  const details = player.videoDetails ?? {};
  const microformat = player.microformat?.playerMicroformatRenderer ?? {};
  const live = microformat.liveBroadcastDetails ?? {};
  const playability = player.playabilityStatus ?? {};
  const playabilityDecision = resolveYoutubePlayability({
    status: playability.status,
    reason: playability.reason ?? (Array.isArray(playability.messages) ? playability.messages.join(" ") : null),
  });
  const contentTypeSignals = extractYoutubePlayerContentTypeSignals(player, {
    source: "youtubei_player",
  });
  const publishedRaw = microformat.publishDate ?? microformat.uploadDate ?? null;
  const liveStatus = contentTypeSignals.is_upcoming === true
    ? "upcoming"
    : contentTypeSignals.is_live === true || contentTypeSignals.is_live_now === true
      ? "is_live"
      : contentTypeSignals.is_live_content === true
        ? "was_live"
        : "not_live";
  const result = mergeDefined({}, {
    title: details.title,
    url,
    thumbnail_url: bestThumbnail(details.thumbnail) ?? bestThumbnail(microformat.thumbnail),
    view_count_text: details.viewCount != null ? String(details.viewCount) : null,
    view_count_source: details.viewCount != null ? "youtubei_player" : null,
    duration_seconds: optionalPositiveInteger(details.lengthSeconds ?? microformat.lengthSeconds),
    length_text: durationTextFromSeconds(details.lengthSeconds ?? microformat.lengthSeconds),
    duration_source: details.lengthSeconds != null || microformat.lengthSeconds != null ? "youtubei_player" : null,
    published_text: dateTextFromIsoLike(publishedRaw),
    published_at: isoToTimestamp(publishedRaw),
    published_at_status: publishedRaw ? "exact" : "unresolved",
    published_at_precision: publishedRaw && /T\d{2}:\d{2}/.test(String(publishedRaw)) ? "second" : publishedRaw ? "date_only" : "unknown",
    published_at_source: publishedRaw ? "youtubei_player_microformat" : null,
    playability_status: player.playabilityStatus?.status ?? null,
    playability_reason: player.playabilityStatus?.reason ?? null,
    playability_kind: playabilityDecision.kind,
    playability_reason_code: playabilityDecision.reason_code,
    playability_retry_mode: playabilityDecision.retry_mode,
    access_status: playabilityDecision.access_status,
    availability: playabilityDecision.availability,
    live_status: liveStatus,
    is_live: liveStatus === "is_live",
    was_live: liveStatus === "was_live",
    is_upcoming: liveStatus === "upcoming",
    live_scheduled_at: liveStatus === "upcoming" ? isoToTimestamp(live.startTimestamp) : null,
    live_started_at: liveStatus !== "upcoming"
      ? isoToTimestamp(live.actualStartTimestamp ?? live.startTimestamp)
      : null,
    live_ended_at: isoToTimestamp(live.actualEndTimestamp ?? live.endTimestamp),
    content_type_signals: contentTypeSignals,
    source: "youtubei_player",
  });
  if (typeof details.shortDescription === "string") {
    result.description = details.shortDescription;
    result.description_source = "youtubei_player";
  }
  result.keywords = normalizeVideoKeywords(details.keywords);
  result.keywords_observed = true;
  return normalizeVideoTextMetadata(result);
}

function watchPageThumbnailUrl(html, videoId) {
  const escaped = String(videoId ?? "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const urls = Array.from(html.matchAll(new RegExp(`"url"\\s*:\\s*"(https://i\\.ytimg\\.com/vi/${escaped}/[^"]+)"`, "g")))
    .map((match) => match[1]?.replace(/\\u0026/g, "&"))
    .filter(Boolean);
  return urls.find((url) => /maxresdefault\.jpg/.test(url))
    ?? urls.find((url) => /hqdefault/.test(url))
    ?? urls[0]
    ?? null;
}

function likeCountFromInitialData(root) {
  const segmented = findFirst(root, "segmentedLikeDislikeButtonViewModel");
  const title = segmented?.likeButtonViewModel?.likeButtonViewModel?.toggleButtonViewModel
    ?.toggleButtonViewModel?.defaultButtonViewModel?.buttonViewModel?.title;
  const exact = parseCountText(title);
  if (exact != null) return exact;
  const candidates = findAll(root, "buttonViewModel")
    .filter((button) => String(button?.iconName ?? "").toUpperCase() === "LIKE")
    .map((button) => parseCountText(button?.title))
    .filter((value) => value != null);
  return candidates.length > 0 ? Math.max(...candidates) : null;
}

function commentCountFromInitialData(root) {
  const header = findFirst(root, "commentsHeaderRenderer");
  const count = parseCountText(textValue(header?.countText));
  if (count != null) return count;
  const commentsEntry = findAll(root, "engagementPanelTitleHeaderRenderer")
    .map((item) => parseCountText(textValue(item?.contextualInfo)))
    .find((value) => value != null);
  return commentsEntry ?? null;
}

function viewCountFromInitialData(root) {
  const renderer = findFirst(root, "videoViewCountRenderer");
  return textValue(renderer?.viewCount)
    ?? textValue(renderer?.shortViewCount)
    ?? (renderer?.originalViewCount && renderer.originalViewCount !== "0" ? String(renderer.originalViewCount) : null);
}

function detailFromWatchPageHtml(html, videoId, url) {
  let initialData = null;
  try {
    initialData = extractYtInitialData(html);
  } catch {
    initialData = null;
  }
  const publishedRaw = jsonStringField(html, "publishDate") ?? jsonStringField(html, "uploadDate");
  const result = mergeDefined({}, {
    title: jsonStringField(html, "title"),
    url,
    thumbnail_url: watchPageThumbnailUrl(html, videoId),
    view_count_text: initialData ? viewCountFromInitialData(initialData) : jsonStringField(html, "viewCount"),
    like_count: initialData ? likeCountFromInitialData(initialData) : optionalInteger(jsonStringField(html, "likeCount")),
    comment_count: initialData ? commentCountFromInitialData(initialData) : optionalInteger(jsonStringField(html, "commentCount")),
    length_text: durationTextFromSeconds(jsonStringField(html, "lengthSeconds")),
    published_text: dateTextFromIsoLike(publishedRaw),
    published_at: isoToTimestamp(publishedRaw),
    canonical_url: canonicalUrlFromHtml(html),
    source: "watch_page",
  });
  const description = jsonStringField(html, "shortDescription");
  if (typeof description === "string") {
    result.description = description;
    result.description_source = "watch_page_player_response";
  }
  return normalizeVideoTextMetadata(result);
}

function innertubePlayerContext(config, language, country) {
  const base = config?.context && typeof config.context === "object" ? config.context : {};
  const baseClient = base.client && typeof base.client === "object" ? base.client : {};
  const clientVersion = baseClient.clientVersion ?? config?.clientVersion ?? "2.20260706.00.00";
  return {
    ...base,
    client: {
      ...baseClient,
      clientName: "WEB",
      clientVersion,
      hl: language || baseClient.hl || DEFAULT_LANGUAGE,
      gl: country || baseClient.gl || DEFAULT_COUNTRY,
      visitorData: baseClient.visitorData ?? config?.visitorData ?? undefined,
    },
  };
}

async function fetchVideoInnertubeDetail(videoId, ytConfig, url, { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY } = {}) {
  const apiKey = String(ytConfig?.apiKey ?? "").trim();
  if (!apiKey) return {};
  const response = await youtubeFetch(`https://www.youtube.com/youtubei/v1/player?key=${encodeURIComponent(apiKey)}`, {
    method: "POST",
    language,
    timeoutMs: 20000,
    headers: {
      "content-type": "application/json",
      "origin": "https://www.youtube.com",
      "referer": url,
      "x-youtube-client-name": String(ytConfig?.clientName ?? "1"),
      "x-youtube-client-version": String(ytConfig?.clientVersion ?? "2.20260706.00.00"),
    },
    body: JSON.stringify({
      context: innertubePlayerContext(ytConfig, language, country),
      videoId,
      playbackContext: { contentPlaybackContext: { html5Preference: "HTML5_PREF_WANTS" } },
      contentCheckOk: true,
      racyCheckOk: true,
    }),
  });
  const text = await response.text();
  if (!response.ok) throw youtubeRequestError(`youtube innertube player failed ${response.status}: ${text.slice(0, 240)}`, {
    status: response.status,
    body: text,
    source: "youtube_innertube_player",
    targetUrl: response.url,
  });
  return detailFromPlayerResponse(JSON.parse(text), url);
}

export function detailFromYtDlpResult(parsed, url = null) {
  if (!parsed?.ok) return {};
  const parsedAvailability = typeof parsed.availability === "string"
    ? parsed.availability.trim().toLowerCase() || null
    : null;
  const parsedAccessStatus = parsedAvailability
    ? videoAccessStatus({ availability: parsedAvailability })
    : "unknown";
  const result = mergeDefined({}, {
    title: parsed.title,
    url: parsed.webpage_url ?? url,
    thumbnail_url: parsed.thumbnail_url,
    view_count_text: parsed.view_count != null ? String(parsed.view_count) : null,
    view_count_source: parsed.view_count != null ? "yt_dlp" : null,
    like_count: optionalInteger(parsed.like_count),
    like_count_source: parsed.like_count != null ? "yt_dlp" : null,
    comment_count: parsed.comments_disabled === true ? 0 : optionalInteger(parsed.comment_count),
    comment_count_status: parsed.comment_count_status,
    comments_disabled: typeof parsed.comments_disabled === "boolean" ? parsed.comments_disabled : null,
    comments_status_source: parsed.comments_status_source,
    comment_count_source: parsed.comments_status_source,
    comments_first_page: parsed.comments_first_page,
    comments_first_page_status: parsed.comments_first_page_status,
    comments_first_page_source: parsed.comments_first_page_source,
    duration_seconds: optionalPositiveInteger(parsed.duration),
    length_text: durationTextFromSeconds(parsed.duration),
    duration_source: parsed.duration != null ? "yt_dlp" : null,
    published_text: dateTextFromIsoLike(parsed.published_text),
    published_at: isoToTimestamp(parsed.published_at ?? parsed.published_text),
    published_at_status: parsed.published_at_status
      ?? (parsed.published_at || parsed.published_text ? "exact" : "unresolved"),
    published_at_precision: parsed.published_at_precision,
    published_at_source: parsed.published_at_source,
    ytdlp_client: parsed.client,
    extractor_version: parsed.extractor_version ?? "v2",
    availability: parsedAvailability,
    playability_status: parsed.playability_status,
    playability_reason: parsed.playability_reason,
    live_status: parsed.live_status,
    is_live: Boolean(parsed.is_live),
    was_live: Boolean(parsed.was_live),
    is_upcoming: ["is_upcoming", "upcoming"].includes(String(parsed.live_status ?? "").toLowerCase()),
    live_scheduled_at: isoToTimestamp(parsed.live_scheduled_at),
    live_started_at: isoToTimestamp(parsed.live_started_at),
    live_ended_at: isoToTimestamp(parsed.live_ended_at),
    channel_id: parsed.channel_id,
    ytdlp_engine: parsed.engine,
    ytdlp_duration_ms: optionalInteger(parsed.duration_ms),
    ytdlp_attempt_timings_ms: parsed.attempt_timings_ms,
    content_type_signals: parsed.content_type_signals,
    source: "yt_dlp",
  });
  const playability = resolveYoutubePlayability({
    status: parsed.playability_status,
    reason: parsed.playability_reason,
  });
  if (parsed.playability_status != null || parsed.playability_reason != null) {
    const explicitAgeRestriction = playability.kind === "content"
      && playability.reason_code === "age_restricted"
      && parsedAccessStatus === "login_required";
    result.playability_kind = playability.kind;
    result.playability_reason_code = playability.reason_code;
    result.playability_retry_mode = playability.retry_mode;
    result.access_status = parsedAccessStatus === "unknown" || explicitAgeRestriction
      ? playability.access_status
      : parsedAccessStatus;
    if (explicitAgeRestriction) result.availability = playability.availability;
    else if (parsedAvailability != null) result.availability = parsedAvailability;
    else if (playability.availability != null) result.availability = playability.availability;
    else if (playability.kind !== "content") delete result.availability;
  }
  if (typeof parsed.description === "string") {
    result.description = parsed.description;
    result.description_source = "yt_dlp";
  }
  result.keywords = normalizeVideoKeywords(parsed.tags);
  result.keywords_observed = true;
  return normalizeVideoTextMetadata(result);
}

export function channelUploadEntryFromYtDlpResult(entry, index = 0) {
  const compactUploadDate = /^\d{8}$/.test(String(entry?.upload_date ?? ""))
    ? `${String(entry.upload_date).slice(0, 4)}-${String(entry.upload_date).slice(4, 6)}-${String(entry.upload_date).slice(6, 8)}`
    : null;
  const timestampSeconds = Number(entry?.timestamp);
  const timestampPublishedAt = Number.isFinite(timestampSeconds) && timestampSeconds > 0
    ? new Date(timestampSeconds * 1000).toISOString()
    : isoToTimestamp(entry?.timestamp);
  const uploadDatePublishedAt = isoToTimestamp(compactUploadDate);
  const publishedAt = timestampPublishedAt ?? uploadDatePublishedAt;
  const liveStatus = String(entry?.live_status ?? "").trim().toLowerCase();
  return {
    video_id: String(entry?.id),
    title: entry?.title ?? null,
    url: `https://www.youtube.com/watch?v=${encodeURIComponent(entry?.id)}`,
    source_url: entry?.url ?? null,
    thumbnail_url: entry?.thumbnail_url ?? null,
    duration_seconds: optionalPositiveInteger(entry?.duration),
    view_count_text: entry?.view_count != null ? String(entry.view_count) : null,
    published_text: dateTextFromIsoLike(publishedAt),
    published_at: publishedAt,
    published_at_status: publishedAt ? "exact" : "unresolved",
    published_at_precision: timestampPublishedAt ? "second" : uploadDatePublishedAt ? "date_only" : "unknown",
    published_at_source: timestampPublishedAt
      ? "yt_dlp_flat_timestamp"
      : uploadDatePublishedAt ? "yt_dlp_flat_upload_date" : null,
    position: Number(entry?.position) || index + 1,
    content_type: ["video", "short", "live"].includes(entry?.content_type) ? entry.content_type : null,
    type_source: entry?.type_source ?? null,
    type_membership: Array.isArray(entry?.type_membership) ? entry.type_membership : [],
    is_live: entry?.is_live === true || liveStatus === "is_live",
    is_upcoming: ["is_upcoming", "upcoming"].includes(liveStatus),
    live_status: liveStatus || null,
  };
}

export async function fetchChannelUploads(channelId, limit = 30, {
  language = DEFAULT_LANGUAGE,
  signal = null,
} = {}) {
  const effectiveSignal = youtubeAdapterAbortSignal(signal);
  throwIfAborted(effectiveSignal);
  const cleanChannelId = String(channelId ?? "").trim();
  if (!cleanChannelId) throw new Error("channel_id is required for uploads playlist");
  const cleanLimit = Math.max(1, Math.min(Number(limit) || 30, 100));
  let parsed;
  try {
    parsed = await persistentChannelUploads(cleanChannelId, cleanLimit, {
      timeoutMs: 180000,
      signal: effectiveSignal,
    });
    throwIfAborted(effectiveSignal);
    if (!parsed) {
      parsed = await runPythonJson(
        YTDLP_UPLOADS_PY,
        { channel_id: cleanChannelId, limit: cleanLimit, language },
        { timeoutMs: 180000, maxBuffer: 16 * 1024 * 1024, signal: effectiveSignal },
      );
      throwIfAborted(effectiveSignal);
    }
  } catch (error) {
    throwIfAborted(effectiveSignal);
    const targetUrl = `https://www.youtube.com/channel/${encodeURIComponent(cleanChannelId)}`;
    throw annotateYtDlpFailure(error, targetUrl, "yt_dlp_uploads");
  }
  throwIfAborted(effectiveSignal);
  if (!parsed?.ok || !Array.isArray(parsed.entries)) {
    const targetUrl = `https://www.youtube.com/channel/${encodeURIComponent(cleanChannelId)}`;
    throw annotateYtDlpFailure(
      new Error(String(parsed?.error ?? "yt-dlp uploads playlist returned no entries")),
      targetUrl,
      "yt_dlp_uploads",
    );
  }
  const rawParseGapCount = Number(parsed.uploads_parse_gap_count);
  const parseGapCount = Number.isInteger(rawParseGapCount) && rawParseGapCount >= 0
    ? rawParseGapCount
    : null;
  const terminalReason = Boolean(parsed.uploads_missing) || parsed.entries.length < cleanLimit
    ? "list_end"
    : "max_items";
  const stopReason = parseGapCount === 0 ? terminalReason : "parse_gap";
  return {
    channel_id: cleanChannelId,
    playlist_id: parsed.playlist_id,
    playlist_url: parsed.playlist_url,
    entries: parsed.entries.slice(0, cleanLimit).map(channelUploadEntryFromYtDlpResult),
    tab_counts: parsed.tab_counts ?? {},
    uploads_missing: Boolean(parsed.uploads_missing),
    activity_evidence_complete: parseGapCount === 0
      && (
        parsed.entries.length > 0
        || Object.values(parsed.tab_counts ?? {}).every((value) => Number(value ?? 0) === 0)
      ),
    activity_parse_gap_count: parseGapCount,
    scan: {
      pages: null,
      inspected_count: parsed.entries.length + (parseGapCount ?? 0),
      parse_gap_count: parseGapCount,
      selected_count: Math.min(parsed.entries.length, cleanLimit),
      stop_reason: stopReason,
      terminal_reason: terminalReason,
      complete: stopReason === "list_end",
    },
    untyped_ids: Array.isArray(parsed.untyped_ids) ? parsed.untyped_ids.map(String) : [],
    raw: parsed,
  };
}

function canonicalUrlFromHtml(html) {
  const match = String(html ?? "").match(/<link\s+rel=["']canonical["']\s+href=["']([^"']+)["']/i)
    ?? String(html ?? "").match(/<link\s+href=["']([^"']+)["']\s+rel=["']canonical["']/i);
  return match?.[1] ? String(match[1]).replaceAll("&amp;", "&") : null;
}

function hasQuickRequiredFields(detail, needsLengthText = true) {
  return Boolean(
    detail
    && Object.keys(detail).length > 0
    && detail.published_text
    && (!needsLengthText || detail.length_text)
  );
}

function needsYtDlpFallback(detail, needsLengthText = true) {
  return !detail?.published_text
    || (needsLengthText && !detail?.length_text)
    || !detail?.view_count_text
    || detail?.like_count == null
    || detail?.comment_count == null;
}

export async function fetchVideoYtDlpDetail(videoId, _itemUrl = null, {
  language = DEFAULT_LANGUAGE,
  signal = null,
} = {}) {
  const effectiveSignal = youtubeAdapterAbortSignal(signal);
  throwIfAborted(effectiveSignal);
  const url = `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`;
  let parsed;
  try {
    parsed = await persistentVideoDetail(url, { timeoutMs: 90000, signal: effectiveSignal });
    throwIfAborted(effectiveSignal);
    if (!parsed) {
      parsed = await runPythonJson(
        YTDLP_QUICK_DETAIL_PY,
        { url, video_id: videoId, language },
        { timeoutMs: 90000, maxBuffer: 12 * 1024 * 1024, signal: effectiveSignal },
      );
      throwIfAborted(effectiveSignal);
    }
  } catch (error) {
    throwIfAborted(effectiveSignal);
    throw annotateYtDlpFailure(error, url, "yt_dlp_detail");
  }
  throwIfAborted(effectiveSignal);
  if (!parsed?.ok) {
    throw annotateYtDlpFailure(
      new Error(String(parsed?.error ?? "yt-dlp returned no detail")),
      url,
      "yt_dlp_detail",
    );
  }
  return assertYoutubeContentObservation(detailFromYtDlpResult(parsed, url), {
    videoId,
    source: "yt_dlp_detail",
  });
}

export async function fetchVideoQuickDetail(
  videoId,
  _itemUrl = null,
  { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY, ytConfig = null, needsLengthText = true } = {},
) {
  const url = `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`;
  let innertubeDetail = {};
  if (ytConfig?.apiKey) {
    try {
      innertubeDetail = await fetchVideoInnertubeDetail(videoId, ytConfig, url, { language, country });
      if (hasQuickRequiredFields(innertubeDetail, needsLengthText)) {
        return innertubeDetail;
      }
    } catch {
      innertubeDetail = {};
    }
  }

  let watchDetail = {};
  let raw = {};
  try {
    const { text } = await youtubeText(url, { language, timeoutMs: 25000 });
    const localYtConfig = ytConfig?.apiKey ? ytConfig : extractYtConfig(text);
    const pagePlayer = detailFromPlayerResponse(extractYtInitialPlayerResponse(text), url);
    const pageDetail = detailFromWatchPageHtml(text, videoId, url);
    watchDetail = mergeDefined(pagePlayer, pageDetail);
    raw = { watch_html: text, detail_json: watchDetail };
    if (!innertubeDetail?.published_text && localYtConfig?.apiKey) {
      try {
        innertubeDetail = mergeDefined(innertubeDetail, await fetchVideoInnertubeDetail(videoId, localYtConfig, url, { language, country }));
      } catch {
        // Keep the watch page result if the player endpoint refuses this client.
      }
    }
  } catch {
    watchDetail = {};
  }

  let detail = mergeDefined(innertubeDetail, watchDetail);
  if (!hasQuickRequiredFields(detail, needsLengthText)) {
    try {
      detail = mergeDefined(detail, await fetchVideoYtDlpDetail(videoId, url, { language }));
    } catch {
      // Data API remains the final fallback in the worker.
    }
  }
  return {
    ...detail,
    _raw: raw,
  };
}

export async function fetchVideoDetail(videoId, _itemUrl = null, { language = DEFAULT_LANGUAGE, country = DEFAULT_COUNTRY } = {}) {
  const url = `https://www.youtube.com/watch?v=${encodeURIComponent(videoId)}`;
  const { text } = await youtubeText(url, { language, timeoutMs: 25000 });
  const ytConfig = extractYtConfig(text);
  const pagePlayer = detailFromPlayerResponse(extractYtInitialPlayerResponse(text), url);
  const pageDetail = detailFromWatchPageHtml(text, videoId, url);
  let innertubeDetail = {};
  try {
    innertubeDetail = await fetchVideoInnertubeDetail(videoId, ytConfig, url, { language, country });
  } catch {
    innertubeDetail = {};
  }
  let detail = mergeDefined(mergeDefined(pagePlayer, innertubeDetail), pageDetail);
  if (needsYtDlpFallback(detail, true)) {
    try {
      detail = mergeDefined(detail, await fetchVideoYtDlpDetail(videoId, url, { language }));
    } catch {
      // Keep the lighter web/Innertube result; worker will queue Data API if fields are still missing.
    }
  }
  return {
    ...detail,
    _raw: {
      watch_html: text,
      detail_json: detail,
    },
  };
}

export function dataApiVideoTargetUrl(videoIds) {
  const ids = Array.isArray(videoIds) ? videoIds : [videoIds];
  return `https://www.googleapis.com/youtube/v3/videos?part=snippet,contentDetails,statistics,status,liveStreamingDetails&id=${encodeURIComponent(ids.filter(Boolean).join(","))}&key=REDACTED`;
}

export function detailFromDataApiItem(item, url = null) {
  if (!item || typeof item !== "object") return {};
  const snippet = item.snippet ?? {};
  const contentDetails = item.contentDetails ?? {};
  const statistics = item.statistics ?? {};
  const hasCommentCount = Object.prototype.hasOwnProperty.call(statistics, "commentCount");
  const liveStreamingDetails = item.liveStreamingDetails ?? {};
  const liveBroadcastContent = String(snippet.liveBroadcastContent ?? "none");
  const isLiveNow = liveBroadcastContent === "live";
  const wasLive = Boolean(liveStreamingDetails.actualEndTime);
  const hasUnresolvedStartedLive = !isLiveNow && !wasLive
    && Boolean(liveStreamingDetails.actualStartTime);
  const isUpcoming = liveBroadcastContent === "upcoming"
    || (!isLiveNow && !wasLive && Boolean(liveStreamingDetails.scheduledStartTime)
      && !liveStreamingDetails.actualStartTime);
  const liveStatus = wasLive
    ? "was_live"
    : isLiveNow
      ? "is_live"
      : isUpcoming ? "upcoming" : hasUnresolvedStartedLive ? null : "not_live";
  const result = mergeDefined({}, {
    title: snippet.title,
    url,
    thumbnail_url: bestThumbnail(snippet.thumbnails ? Object.values(snippet.thumbnails) : null),
    view_count_text: statistics.viewCount != null ? String(statistics.viewCount) : null,
    view_count_source: statistics.viewCount != null ? "youtube_data_api_statistics" : null,
    like_count: optionalInteger(statistics.likeCount),
    like_count_source: statistics.likeCount != null ? "youtube_data_api_statistics" : null,
    comment_count: hasCommentCount
      ? optionalInteger(statistics.commentCount)
      : item.status?.privacyStatus === "public" ? 0 : null,
    comment_count_status: hasCommentCount ? "exact" : item.status?.privacyStatus === "public" ? "disabled" : "unresolved",
    comments_disabled: item.status?.privacyStatus === "public" ? !hasCommentCount : null,
    comments_status_source: item.status?.privacyStatus === "public" ? "youtube_data_api_statistics" : null,
    comment_count_source: item.status?.privacyStatus === "public" ? "youtube_data_api_statistics" : null,
    duration_seconds: secondsFromIsoDuration(contentDetails.duration),
    length_text: durationTextFromIsoDuration(contentDetails.duration),
    duration_source: contentDetails.duration ? "youtube_data_api_content_details" : null,
    published_text: dateTextFromIsoLike(snippet.publishedAt),
    published_at: isoToTimestamp(snippet.publishedAt),
    published_at_status: snippet.publishedAt ? "exact" : "unresolved",
    published_at_precision: snippet.publishedAt ? "second" : "unknown",
    published_at_source: snippet.publishedAt ? "youtube_data_api_snippet" : null,
    live_status: liveStatus,
    is_live: isLiveNow,
    was_live: wasLive ? true : hasUnresolvedStartedLive ? null : false,
    is_upcoming: isUpcoming,
    privacy_status: item.status?.privacyStatus ?? null,
    embeddable: item.status?.embeddable ?? null,
    live_scheduled_at: isoToTimestamp(liveStreamingDetails.scheduledStartTime),
    live_started_at: isoToTimestamp(liveStreamingDetails.actualStartTime),
    live_ended_at: isoToTimestamp(liveStreamingDetails.actualEndTime),
    extractor_version: "data-api-v1",
    source: "youtube_data_api_videos_list",
  });
  if (typeof snippet.description === "string") {
    result.description = snippet.description;
    result.description_source = "youtube_data_api_snippet";
  }
  result.keywords = normalizeVideoKeywords(snippet.tags);
  result.keywords_observed = true;
  return normalizeVideoTextMetadata(result, { afterApi: true });
}

export async function fetchVideoDataApiDetails(videoIds, apiKey, { timeoutMs = 12000 } = {}) {
  const cleanVideoIds = Array.from(new Set((Array.isArray(videoIds) ? videoIds : [videoIds])
    .map((id) => String(id ?? "").trim())
    .filter(Boolean)))
    .slice(0, 50);
  const cleanApiKey = String(apiKey ?? "").trim();
  if (cleanVideoIds.length === 0) return { detailsById: new Map(), raw: { items: [] }, targetUrl: dataApiVideoTargetUrl([]) };
  if (!cleanApiKey) throw new Error("youtube data api key missing");
  const params = new URLSearchParams({
    part: "snippet,contentDetails,statistics,status,liveStreamingDetails",
    id: cleanVideoIds.join(","),
    key: cleanApiKey,
  });
  const response = await persistentFetch(`https://www.googleapis.com/youtube/v3/videos?${params.toString()}`, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!response.ok) {
    const apiMessage = json?.error?.message ? `: ${json.error.message}` : "";
    throw new Error(`youtube data api videos.list failed ${response.status} ${response.statusText}${apiMessage}`);
  }
  const detailsById = new Map();
  for (const item of Array.isArray(json?.items) ? json.items : []) {
    const id = String(item?.id ?? "").trim();
    if (!id) continue;
    detailsById.set(id, detailFromDataApiItem(item, `https://www.youtube.com/watch?v=${encodeURIComponent(id)}`));
  }
  return {
    detailsById,
    raw: json,
    targetUrl: dataApiVideoTargetUrl(cleanVideoIds),
    returnedCount: Array.isArray(json?.items) ? json.items.length : 0,
  };
}

export function dataApiCommentThreadsTargetUrl(videoId) {
  return `https://www.googleapis.com/youtube/v3/commentThreads?part=snippet&videoId=${encodeURIComponent(String(videoId ?? ""))}&order=relevance&maxResults=20&textFormat=plainText&key=REDACTED`;
}

export async function fetchVideoCommentThreadsDataApi(videoId, apiKey, {
  timeoutMs = 12000,
  totalCount = null,
  collectedAt = new Date(),
  retryDays = 7,
  fetchImpl = persistentFetch,
} = {}) {
  const cleanVideoId = String(videoId ?? "").trim();
  const cleanApiKey = String(apiKey ?? "").trim();
  if (!cleanVideoId) throw new Error("video_id is required");
  if (!cleanApiKey) throw new Error("youtube data api key missing");
  const params = new URLSearchParams({
    part: "snippet",
    videoId: cleanVideoId,
    order: "relevance",
    maxResults: "20",
    textFormat: "plainText",
    key: cleanApiKey,
  });
  const response = await fetchImpl(`https://www.googleapis.com/youtube/v3/commentThreads?${params.toString()}`, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const responseText = await response.text();
  let json = null;
  try {
    json = responseText ? JSON.parse(responseText) : null;
  } catch {
    json = null;
  }
  const reasons = (Array.isArray(json?.error?.errors) ? json.error.errors : [])
    .map((item) => String(item?.reason ?? "").trim())
    .filter(Boolean);
  if (!response.ok && reasons.includes("commentsDisabled")) {
    const page = {
      ...emptyYoutubeCommentPage({ collectedAt, totalCount: 0 }),
      resolution: {
        status: "disabled",
        checked_at: new Date(collectedAt).toISOString(),
        sources: ["youtube_data_api_comment_threads"],
      },
    };
    return {
      status: "disabled",
      raw: json,
      targetUrl: dataApiCommentThreadsTargetUrl(cleanVideoId),
      detail: {
        comments_disabled: true,
        comment_count: 0,
        comment_count_status: "disabled",
        comments_status_source: "youtube_data_api_comment_threads",
        comment_count_source: "youtube_data_api_comment_threads",
        comments_first_page: page,
        comments_first_page_status: "disabled",
        comments_first_page_source: "youtube_data_api_comment_threads",
      },
    };
  }
  if (!response.ok) {
    const apiMessage = json?.error?.message ? `: ${json.error.message}` : "";
    const error = new Error(
      `youtube data api commentThreads.list failed ${response.status} ${response.statusText}${apiMessage}`,
    );
    error.code = reasons[0] || `HTTP_${response.status}`;
    error.api_reasons = reasons;
    throw error;
  }
  const page = commentPageFromDataApiThreads(json, { collectedAt, totalCount });
  const hasRows = Number(page.returned_count) > 0;
  const checkedAt = new Date(collectedAt);
  const confirmedPage = hasRows
    ? page
    : confirmedNoVisibleThreadsPage({
        collectedAt: checkedAt,
        checkedAt,
        retryAt: new Date(checkedAt.getTime() + (Math.max(1, Number(retryDays) || 7) * 86400000)),
        totalCount,
        sources: [
          "yt_dlp_top_comments",
          "youtubejs_comments",
          "youtube_data_api_comment_threads",
        ],
      });
  return {
    status: hasRows ? "collected" : "confirmed_no_visible_threads",
    raw: json,
    targetUrl: dataApiCommentThreadsTargetUrl(cleanVideoId),
    detail: {
      comments_disabled: false,
      comment_count: totalCount,
      comment_count_status: totalCount == null ? "unresolved" : "exact",
      comments_status_source: "youtube_data_api_comment_threads",
      comment_count_source: totalCount == null ? null : "youtube_data_api_statistics",
      comments_first_page: confirmedPage,
      comments_first_page_status: hasRows ? "collected" : "confirmed_no_visible_threads",
      comments_first_page_source: "youtube_data_api_comment_threads",
    },
  };
}

export function dataApiChannelTargetUrl(channelIds) {
  const ids = Array.isArray(channelIds) ? channelIds : [channelIds];
  return `https://www.googleapis.com/youtube/v3/channels?part=statistics,snippet&id=${encodeURIComponent(ids.filter(Boolean).join(","))}&key=REDACTED`;
}

export function channelDetailFromDataApiItem(item) {
  const id = String(item?.id ?? "").trim();
  if (!id) return null;
  const subCount = optionalInteger(item?.statistics?.subscriberCount);
  return {
    channel_id: id,
    title: item?.snippet?.title ?? null,
    handle: item?.snippet?.customUrl ? `@${String(item.snippet.customUrl).replace(/^@/, "")}` : null,
    description: item?.snippet?.description ?? null,
    avatar_url: bestThumbnail(item?.snippet?.thumbnails ? Object.values(item.snippet.thumbnails) : null),
    subscriber_count: subCount,
    subscriber_count_text: subCount != null ? `${subCount} subscribers` : null,
    subscriber_count_source: subCount != null ? "youtube_data_api_channels_list" : null,
    hidden_subscriber_count: Boolean(item?.statistics?.hiddenSubscriberCount),
    source: "youtube_data_api_channels_list",
  };
}

export async function fetchChannelDataApiDetails(channelIds, apiKey, { timeoutMs = 12000 } = {}) {
  const cleanChannelIds = Array.from(new Set((Array.isArray(channelIds) ? channelIds : [channelIds])
    .map((id) => String(id ?? "").trim())
    .filter(Boolean)))
    .slice(0, 50);
  const cleanApiKey = String(apiKey ?? "").trim();
  if (cleanChannelIds.length === 0) return { detailsById: new Map(), raw: { items: [] }, targetUrl: dataApiChannelTargetUrl([]) };
  if (!cleanApiKey) throw new Error("youtube data api key missing");
  const params = new URLSearchParams({
    part: "statistics,snippet",
    id: cleanChannelIds.join(","),
    key: cleanApiKey,
  });
  const response = await persistentFetch(`https://www.googleapis.com/youtube/v3/channels?${params.toString()}`, {
    headers: { accept: "application/json" },
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await response.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = null;
  }
  if (!response.ok) {
    const apiMessage = json?.error?.message ? `: ${json.error.message}` : "";
    throw new Error(`youtube data api channels.list failed ${response.status} ${response.statusText}${apiMessage}`);
  }
  const detailsById = new Map();
  for (const item of Array.isArray(json?.items) ? json.items : []) {
    const detail = channelDetailFromDataApiItem(item);
    if (detail) detailsById.set(detail.channel_id, detail);
  }
  return {
    detailsById,
    raw: json,
    targetUrl: dataApiChannelTargetUrl(cleanChannelIds),
    returnedCount: Array.isArray(json?.items) ? json.items.length : 0,
  };
}

export async function fetchChannelYtDlpMetadata(channelUrl, {
  language = DEFAULT_LANGUAGE,
  signal = null,
} = {}) {
  const effectiveSignal = youtubeAdapterAbortSignal(signal);
  throwIfAborted(effectiveSignal);
  let parsed = await persistentChannelMetadata(channelUrl, {
    timeoutMs: 90000,
    signal: effectiveSignal,
  });
  throwIfAborted(effectiveSignal);
  if (!parsed) {
    parsed = await runPythonJson(
      YTDLP_CHANNEL_METADATA_PY,
      { url: channelUrl, language },
      { timeoutMs: 90000, maxBuffer: 12 * 1024 * 1024, signal: effectiveSignal },
    );
    throwIfAborted(effectiveSignal);
  }
  if (!parsed?.ok) throw new Error(String(parsed?.error ?? "yt-dlp returned no channel metadata"));
  const followerCount = optionalInteger(parsed.channel_follower_count);
  return mergeDefined({}, {
    channel_id: parsed.channel_id ?? parsed.id,
    title: parsed.title ?? parsed.channel ?? parsed.uploader,
    handle: parsed.uploader_id?.startsWith("@") ? parsed.uploader_id : null,
    channel_url: parsed.channel_url,
    vanity_channel_url: parsed.uploader_url ?? parsed.webpage_url,
    description: parsed.description,
    avatar_url: parsed.avatar_url,
    subscriber_count: followerCount,
    subscriber_count_text: followerCount != null ? `${followerCount} subscribers` : null,
    subscriber_count_source: followerCount != null ? "yt_dlp_channel_follower_count" : null,
    source: "yt_dlp_channel_metadata",
    raw: parsed,
  });
}

function extractPublishedText(parts, locale = DEFAULT_LANGUAGE) {
  for (const part of parts) {
    if (parseLocalizedAgeDays(part, { locale }) != null) return String(part);
  }
  return null;
}

export function publishedAgeDays(text, locale = DEFAULT_LANGUAGE) {
  return parseLocalizedAgeDays(text, { locale });
}

export function isWithinRecentWindow(item, recentDays = DEFAULT_RECENT_DAYS, locale = DEFAULT_LANGUAGE) {
  if (!recentDays || recentDays <= 0) return true;
  const days = publishedAgeDays(item?.published_text, locale);
  if (days == null) return true;
  return days <= recentDays;
}

export function isOlderThanRecentWindow(item, recentDays = DEFAULT_RECENT_DAYS, locale = DEFAULT_LANGUAGE) {
  if (!recentDays || recentDays <= 0) return false;
  const days = publishedAgeDays(item?.published_text, locale);
  return days != null && days > recentDays;
}

function parseVideoRenderer(renderer, fallbackType = "videos") {
  const videoId = renderer.videoId;
  if (!videoId) return null;
  return {
    video_id: videoId,
    type: fallbackType,
    title: textValue(renderer.title),
    url: absoluteYoutubeUrl(renderer.navigationEndpoint?.commandMetadata?.webCommandMetadata?.url ?? `/watch?v=${videoId}`),
    thumbnail_url: bestThumbnail(renderer.thumbnail),
    length_text: textValue(renderer.lengthText) ?? textValue(renderer.thumbnailOverlays?.find((overlay) => overlay.thumbnailOverlayTimeStatusRenderer)?.thumbnailOverlayTimeStatusRenderer?.text),
    view_count_text: textValue(renderer.viewCountText) ?? textValue(renderer.shortViewCountText),
    like_count: null,
    comment_count: null,
    published_text: textValue(renderer.publishedTimeText),
    raw: renderer,
  };
}

function parseLockupVideo(lockup, fallbackType = "videos", locale = DEFAULT_LANGUAGE) {
  const videoId = lockup?.contentId
    ?? lockup?.rendererContext?.commandContext?.onTap?.innertubeCommand?.watchEndpoint?.videoId;
  if (!videoId || lockup?.contentType !== "LOCKUP_CONTENT_TYPE_VIDEO") return null;
  const meta = lockup.metadata?.lockupMetadataViewModel;
  const parts = meta?.metadata?.contentMetadataViewModel?.metadataRows
    ?.flatMap((row) => row?.metadataParts ?? [])
    ?.map((part) => textValue(part.text) ?? part.accessibilityLabel ?? null)
    ?.filter(Boolean) ?? [];
  return {
    video_id: videoId,
    type: fallbackType,
    title: textValue(meta?.title),
    url: absoluteYoutubeUrl(lockup.rendererContext?.commandContext?.onTap?.innertubeCommand?.commandMetadata?.webCommandMetadata?.url ?? `/watch?v=${videoId}`),
    thumbnail_url: bestThumbnail(lockup.contentImage?.thumbnailViewModel?.image),
    length_text: lockup.contentImage?.thumbnailViewModel?.overlays
      ?.flatMap((overlay) => overlay?.thumbnailBottomOverlayViewModel?.badges ?? [])
      ?.map((badge) => badge?.thumbnailBadgeViewModel?.text ?? null)
      ?.find(Boolean) ?? null,
    view_count_text: parts.find((part) => /visualiza|view/i.test(part)) ?? null,
    like_count: null,
    comment_count: null,
    published_text: extractPublishedText(parts, locale),
    raw: lockup,
  };
}

function parseShortsLockup(item, locale = DEFAULT_LANGUAGE) {
  const videoId = item?.onTap?.innertubeCommand?.reelWatchEndpoint?.videoId
    ?? item?.onTap?.innertubeCommand?.commandMetadata?.webCommandMetadata?.url?.split("/shorts/")?.[1]?.split(/[?&/]/)?.[0];
  if (!videoId) return null;
  const accessibilityText = item.accessibilityText ?? null;
  const parts = typeof accessibilityText === "string" ? accessibilityText.split(",").map((part) => part.trim()) : [];
  const viewCountIndex = parts.findIndex((part) => /visualiza|view/i.test(part));
  return {
    video_id: videoId,
    type: "shorts",
    title: typeof accessibilityText === "string" ? accessibilityText.split(",")[0]?.trim() ?? null : null,
    url: absoluteYoutubeUrl(item?.onTap?.innertubeCommand?.commandMetadata?.webCommandMetadata?.url ?? `/shorts/${videoId}`),
    thumbnail_url: bestThumbnail(item?.onTap?.innertubeCommand?.reelWatchEndpoint?.thumbnail),
    length_text: null,
    view_count_text: viewCountIndex >= 0 ? parts[viewCountIndex] : null,
    like_count: null,
    comment_count: null,
    published_text: extractPublishedText(
      viewCountIndex >= 0 ? parts.slice(viewCountIndex + 1) : parts.slice(1),
      locale,
    ),
    raw: item,
  };
}

export function extractTabItems(root, tab, recentDays = DEFAULT_RECENT_DAYS, locale = DEFAULT_LANGUAGE) {
  const seen = new Set();
  const out = [];
  const fallbackType = tab === "shorts" ? "shorts" : tab === "streams" || tab === "lives" ? "lives" : "videos";
  let unknownPublishedCount = 0;
  let inspectedCount = 0;
  const add = (item) => {
    if (!item?.video_id || seen.has(item.video_id)) return false;
    inspectedCount += 1;
    const ageDays = item.published_text == null
      ? null
      : parseRequiredLocalizedAgeDays(item.published_text, {
          locale,
          field: "published_age",
          source: `youtube_channel_tab:${tab}`,
          context: { video_id: item.video_id },
        });
    if (ageDays == null) unknownPublishedCount += 1;
    if (recentDays > 0 && ageDays != null && ageDays > recentDays) return "cutoff";
    seen.add(item.video_id);
    out.push(item);
    return true;
  };

  let cutoff = false;
  const rawSources = tab === "shorts"
    ? findAll(root, "shortsLockupViewModel").map((item) => parseShortsLockup(item, locale))
    : [
        ...findAll(root, "lockupViewModel").map((item) => parseLockupVideo(item, fallbackType, locale)),
        ...findAll(root, "videoRenderer").map((item) => parseVideoRenderer(item, fallbackType)),
      ];
  const sourceSeen = new Set();
  const sources = [];
  for (const item of rawSources) {
    if (!item?.video_id || sourceSeen.has(item.video_id)) continue;
    sourceSeen.add(item.video_id);
    sources.push(item);
  }
  for (const item of sources) {
    const result = add(item);
    if (result === "cutoff") {
      cutoff = true;
      break;
    }
  }
  const ageUnknownRatio = inspectedCount > 0 ? unknownPublishedCount / inspectedCount : 0;
  return {
    items: out,
    page_items: sources,
    recent_cutoff_hit: cutoff,
    age_unknown_page: tab === "shorts" && inspectedCount > 0 && ageUnknownRatio > 0.5,
    age_unknown_ratio: ageUnknownRatio,
    inspected_count: inspectedCount,
    unknown_published_count: unknownPublishedCount,
    continuation_token: findContinuationToken(root),
  };
}

export function normalizeContentType(type) {
  if (type === "shorts") return "short";
  if (type === "lives" || type === "streams") return "live";
  return "video";
}
