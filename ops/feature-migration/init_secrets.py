from __future__ import annotations

import os
from pathlib import Path
import secrets
import tempfile
from urllib.parse import quote


ROOT = Path("/runtime-secrets")
DATABASE_HOST = str(
    os.environ.get("FEATURE_DATABASE_HOST") or "bullmq-crawler-migration-pgbouncer"
).strip()
DATABASE_PORT = str(os.environ.get("FEATURE_DATABASE_PORT") or "6432").strip()
DATABASE_NAME = str(
    os.environ.get("FEATURE_DATABASE_NAME") or "bullmq_crawler_migration"
).strip()
DATABASE_USER = str(os.environ.get("FEATURE_DATABASE_USER") or "feature_user").strip()
ADMIN_DATABASE_HOST = str(
    os.environ.get("FEATURE_ADMIN_DATABASE_HOST")
    or "bullmq-crawler-migration-postgres"
).strip()
ADMIN_DATABASE_PORT = str(
    os.environ.get("FEATURE_ADMIN_DATABASE_PORT") or "5432"
).strip()
LEGACY_DATABASE_HOST = str(
    os.environ.get("LEGACY_FEATURE_DATABASE_HOST") or "feature-postgres"
).strip()
LEGACY_DATABASE_NAME = str(
    os.environ.get("LEGACY_FEATURE_DATABASE_NAME") or "feature_clock"
).strip()


def read_existing(name: str) -> str | None:
    path = ROOT / name
    try:
        value = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not value:
        raise RuntimeError(f"runtime secret {name} is empty")
    return value


def write_atomic(name: str, value: str, mode: int) -> None:
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = ROOT / name
    current = read_existing(name)
    if current == value:
        os.chmod(target, mode)
        return
    descriptor, temporary = tempfile.mkstemp(prefix=f".{name}.", dir=ROOT)
    try:
        os.write(descriptor, f"{value}\n".encode("utf-8"))
        os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)
    os.replace(temporary, target)


def generated(name: str) -> str:
    existing = read_existing(name)
    if existing is not None:
        return existing
    value = secrets.token_urlsafe(48)
    write_atomic(name, value, 0o444)
    return value


def database_url(host: str, port: str, password: str) -> str:
    return (
        f"postgresql://{quote(DATABASE_USER, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{DATABASE_NAME}"
    )


def main() -> None:
    password = generated("feature_db_password")
    generated("feature_ingest_token")
    runtime_url = database_url(DATABASE_HOST, DATABASE_PORT, password)
    admin_url = database_url(ADMIN_DATABASE_HOST, ADMIN_DATABASE_PORT, password)
    pgpass_entries = dict.fromkeys(
        (
            f"{DATABASE_HOST}:{DATABASE_PORT}:{DATABASE_NAME}:{DATABASE_USER}:{password}",
            f"{ADMIN_DATABASE_HOST}:{ADMIN_DATABASE_PORT}:{DATABASE_NAME}:{DATABASE_USER}:{password}",
            f"{LEGACY_DATABASE_HOST}:5432:{LEGACY_DATABASE_NAME}:{DATABASE_USER}:{password}",
        )
    )
    pgpass = "\n".join(pgpass_entries)
    write_atomic("feature_database_url", runtime_url, 0o444)
    write_atomic("feature_admin_database_url", admin_url, 0o444)
    write_atomic("feature_pgpass", pgpass, 0o600)
    print("feature runtime secrets ready", flush=True)


if __name__ == "__main__":
    main()
