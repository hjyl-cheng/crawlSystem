from __future__ import annotations

from dataclasses import asdict, dataclass
from hmac import compare_digest
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import signal
import threading
from typing import Any, Callable, Mapping

from .applier import EventConflict, FeatureObservationApplier, FeatureStateInvariantError
from .database_topology import validate_shared_feature_database
from .events import EventValidationError
from .runtime_environment import optional_environment, required_environment


@dataclass(frozen=True, slots=True)
class IngestResponse:
    status: int
    body: dict[str, Any]


class FeatureIngestApplication:
    def __init__(
        self,
        applier: FeatureObservationApplier,
        *,
        token: str | None,
        readiness: Callable[[], Mapping[str, Any]] | None = None,
        max_body_bytes: int = 65536,
    ) -> None:
        self._applier = applier
        self._token = str(token or "").strip() or None
        self._readiness = readiness
        self.max_body_bytes = max(1024, min(int(max_body_bytes), 1048576))

    def _authorized(self, headers: Mapping[str, str]) -> bool:
        if self._token is None:
            return True
        authorization = str(headers.get("authorization", ""))
        expected = f"Bearer {self._token}"
        return compare_digest(authorization, expected)

    def handle(
        self,
        *,
        method: str,
        path: str,
        headers: Mapping[str, str] | None = None,
        body: bytes = b"",
    ) -> IngestResponse:
        normalized_headers = {str(key).lower(): str(value) for key, value in (headers or {}).items()}
        if method == "GET" and path == "/healthz":
            return IngestResponse(HTTPStatus.OK, {"ok": True, "status": "alive"})
        if method == "GET" and path == "/readyz":
            try:
                details = dict(self._readiness() if self._readiness else {})
                return IngestResponse(HTTPStatus.OK, {"ok": True, "status": "ready", **details})
            except Exception:
                return IngestResponse(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"ok": False, "error": "feature database is not ready"},
                )
        if path != "/v1/crawler-observations":
            return IngestResponse(HTTPStatus.NOT_FOUND, {"ok": False, "error": "not found"})
        if method != "POST":
            return IngestResponse(
                HTTPStatus.METHOD_NOT_ALLOWED,
                {"ok": False, "error": "method not allowed"},
            )
        if not self._authorized(normalized_headers):
            return IngestResponse(HTTPStatus.UNAUTHORIZED, {"ok": False, "error": "unauthorized"})
        if not normalized_headers.get("content-type", "").lower().startswith("application/json"):
            return IngestResponse(
                HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                {"ok": False, "error": "content-type must be application/json"},
            )
        if len(body) > self.max_body_bytes:
            return IngestResponse(
                HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                {"ok": False, "error": "request body is too large"},
            )
        try:
            event = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return IngestResponse(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid JSON"})
        if not isinstance(event, Mapping):
            return IngestResponse(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "event must be an object"})
        event_id = str(event.get("event_id") or "").strip()
        idempotency_key = normalized_headers.get("idempotency-key", "").strip()
        if not idempotency_key or idempotency_key != event_id:
            return IngestResponse(
                HTTPStatus.BAD_REQUEST,
                {"ok": False, "error": "Idempotency-Key must equal event_id"},
            )
        try:
            result = self._applier.apply_crawler_observation(event)
        except EventValidationError as error:
            return IngestResponse(
                HTTPStatus.UNPROCESSABLE_ENTITY,
                {"ok": False, "error": str(error)},
            )
        except EventConflict as error:
            return IngestResponse(HTTPStatus.CONFLICT, {"ok": False, "error": str(error)})
        except FeatureStateInvariantError:
            return IngestResponse(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"ok": False, "error": "feature state is temporarily unavailable"},
            )
        except Exception:
            return IngestResponse(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"ok": False, "error": "feature ingest temporarily failed"},
            )
        payload = asdict(result)
        payload["drained_event_ids"] = list(result.drained_event_ids)
        return IngestResponse(
            HTTPStatus.ACCEPTED if result.status == "waiting_gap" else HTTPStatus.OK,
            {"ok": True, **payload},
        )


def _handler(application: FeatureIngestApplication) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        server_version = "qy-feature-ingest/1"

        def _dispatch(self) -> None:
            try:
                content_length = int(self.headers.get("content-length", "0"))
            except ValueError:
                content_length = -1
            if content_length < 0:
                response = IngestResponse(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "invalid content-length"})
            elif content_length > application.max_body_bytes:
                response = IngestResponse(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    {"ok": False, "error": "request body is too large"},
                )
            else:
                body = self.rfile.read(content_length) if content_length else b""
                response = application.handle(
                    method=self.command,
                    path=self.path.split("?", 1)[0],
                    headers=dict(self.headers.items()),
                    body=body,
                )
            encoded = json.dumps(response.body, separators=(",", ":")).encode("utf-8")
            self.send_response(int(response.status))
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def log_message(self, format: str, *args: Any) -> None:
            return

    return Handler


def _loopback_bind(host: str) -> bool:
    return host in {"127.0.0.1", "localhost", "::1"}


def main() -> None:
    import psycopg

    database_url = required_environment(os.environ, "FEATURE_DATABASE_URL")
    expected_database = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE")
    expected_user = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE_USER")
    bind_host = str(os.environ.get("FEATURE_INGEST_HOST") or "127.0.0.1").strip()
    bind_port = int(os.environ.get("FEATURE_INGEST_PORT") or "8090")
    token = optional_environment(os.environ, "FEATURE_INGEST_TOKEN")
    allow_insecure = str(os.environ.get("FEATURE_ALLOW_INSECURE_LOCALHOST") or "").lower() == "true"
    if token is None and not (_loopback_bind(bind_host) and allow_insecure):
        raise RuntimeError("FEATURE_INGEST_TOKEN is required unless insecure loopback is explicitly enabled")

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    def readiness() -> Mapping[str, Any]:
        identity = validate_shared_feature_database(
            connect,
            expected_database=expected_database,
            expected_user=expected_user,
            required_feature_relations=("feature_clock.crawler_event_inbox",),
        )
        return {
            "database": identity.database,
            "database_user": identity.user,
            "topology": "shared_crawler_database",
        }

    readiness()
    application = FeatureIngestApplication(
        FeatureObservationApplier(connect),
        token=token,
        readiness=readiness,
        max_body_bytes=int(os.environ.get("FEATURE_INGEST_MAX_BODY_BYTES") or "65536"),
    )
    server = ThreadingHTTPServer((bind_host, bind_port), _handler(application))

    def stop_server(_signum: int, _frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, stop_server)
    signal.signal(signal.SIGTERM, stop_server)
    print(
        json.dumps(
            {
                "event": "feature_ingest_ready",
                "host": bind_host,
                "port": bind_port,
                "database": expected_database,
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
