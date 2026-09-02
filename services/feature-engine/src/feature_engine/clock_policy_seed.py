from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .contracts import ActivePolicyContract, validate_active_policy_contract
from .database_topology import validate_shared_feature_database
from .runtime_environment import required_environment


DEFAULT_POLICY_FILE = Path(__file__).with_name("clock_policy_v16_rule_7.json")
MANIFEST_FIELDS = frozenset(
    {"schema_version", "domain_rule_versions", "source_sha256", "policy"}
)
POLICY_FIELDS = frozenset(
    {
        "policy_version",
        "status",
        "effective_from",
        "allowed_days",
        "about_config",
        "discovery_config",
        "recent_sampling_config",
        "agent_config",
        "partial_retry_config",
        "checksum",
    }
)
DOMAIN_FIELDS = frozenset({"about", "video", "agent"})
CONFIG_FIELDS = (
    "about_config",
    "discovery_config",
    "recent_sampling_config",
    "agent_config",
    "partial_retry_config",
)


class ClockPolicySeedError(RuntimeError):
    pass


class ClockPolicySeedConflict(ClockPolicySeedError):
    pass


@dataclass(frozen=True, slots=True)
class ClockPolicyManifest:
    source_path: Path
    source_sha256: str
    domain_rule_versions: Mapping[str, str]
    policy: Mapping[str, Any]
    contract: ActivePolicyContract

    @property
    def policy_version(self) -> str:
        return self.contract.policy_version


@dataclass(frozen=True, slots=True)
class ClockPolicySeedPlan:
    action: str
    database: str
    policy_version: str
    previous_active_policy: str | None
    source_sha256: str
    confirmation: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "database": self.database,
            "policy_version": self.policy_version,
            "previous_active_policy": self.previous_active_policy,
            "source_sha256": self.source_sha256,
            "confirmation": self.confirmation,
        }


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _mapping(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ClockPolicySeedError(f"{field} must be an object")
    return dict(value)


def _exact_fields(value: Mapping[str, Any], expected: frozenset[str], field: str) -> None:
    actual = frozenset(value)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ClockPolicySeedError(
            f"{field} fields differ: missing={missing}, extra={extra}"
        )


def _normalized_timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value or "").strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as error:
            raise ClockPolicySeedError("policy.effective_from must be ISO-8601") from error
    if parsed.tzinfo is None:
        raise ClockPolicySeedError("policy.effective_from must include a timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def load_clock_policy_manifest(
    source_path: str | os.PathLike[str] = DEFAULT_POLICY_FILE,
) -> ClockPolicyManifest:
    path = Path(source_path)
    try:
        source = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ClockPolicySeedError(f"cannot load Clock policy manifest: {path}") from error
    manifest = _mapping(source, "manifest")
    _exact_fields(manifest, MANIFEST_FIELDS, "manifest")
    if manifest["schema_version"] != 1:
        raise ClockPolicySeedError("manifest.schema_version must equal 1")

    domains = _mapping(manifest["domain_rule_versions"], "domain_rule_versions")
    _exact_fields(domains, DOMAIN_FIELDS, "domain_rule_versions")
    if any(not str(value).strip() for value in domains.values()):
        raise ClockPolicySeedError("domain_rule_versions values must be non-empty")

    policy = _mapping(manifest["policy"], "policy")
    _exact_fields(policy, POLICY_FIELDS, "policy")
    if policy["status"] != "active":
        raise ClockPolicySeedError("policy.status must equal active")
    policy["effective_from"] = _normalized_timestamp(policy["effective_from"])
    contract = validate_active_policy_contract(policy)

    source_facts = {"domain_rule_versions": domains, "policy": policy}
    actual_sha256 = "sha256:" + sha256(_canonical_json(source_facts)).hexdigest()
    if manifest["source_sha256"] != actual_sha256:
        raise ClockPolicySeedError(
            f"manifest source_sha256 mismatch: expected {actual_sha256}"
        )
    return ClockPolicyManifest(
        source_path=path,
        source_sha256=actual_sha256,
        domain_rule_versions={key: str(value) for key, value in domains.items()},
        policy=policy,
        contract=contract,
    )


def _policy_facts(row: Mapping[str, Any]) -> dict[str, Any]:
    facts = {field: row.get(field) for field in POLICY_FIELDS if field != "status"}
    facts["effective_from"] = _normalized_timestamp(facts["effective_from"])
    facts["allowed_days"] = list(facts["allowed_days"] or [])
    for field in CONFIG_FIELDS:
        value = facts[field]
        if isinstance(value, str):
            value = json.loads(value)
        facts[field] = dict(value or {})
    return facts


def plan_clock_policy_seed(
    rows: Sequence[Mapping[str, Any]],
    manifest: ClockPolicyManifest,
    *,
    database: str,
) -> ClockPolicySeedPlan:
    active_rows = [row for row in rows if row.get("status") == "active"]
    if len(active_rows) > 1:
        raise ClockPolicySeedConflict("multiple active Clock policies found")
    active_version = (
        str(active_rows[0].get("policy_version")) if active_rows else None
    )
    target = next(
        (
            row
            for row in rows
            if str(row.get("policy_version") or "") == manifest.policy_version
        ),
        None,
    )
    if target is None:
        action = "insert_and_activate"
    else:
        expected = _policy_facts(manifest.policy)
        actual = _policy_facts(target)
        if actual != expected:
            raise ClockPolicySeedConflict(
                f"stored policy {manifest.policy_version} differs from the local manifest"
            )
        status = str(target.get("status") or "")
        if status == "active":
            action = "already_active"
        elif status == "draft":
            action = "activate_draft"
        else:
            raise ClockPolicySeedConflict(
                f"stored policy {manifest.policy_version} cannot be reactivated from {status}"
            )
    confirmation = (
        f"ACTIVATE_CLOCK_POLICY:{database}:{manifest.policy_version}:"
        f"{manifest.source_sha256.removeprefix('sha256:')[:16]}"
    )
    return ClockPolicySeedPlan(
        action=action,
        database=database,
        policy_version=manifest.policy_version,
        previous_active_policy=active_version,
        source_sha256=manifest.source_sha256,
        confirmation=confirmation,
    )


def _load_policy_rows(connection: Any, *, for_update: bool = False) -> list[dict[str, Any]]:
    suffix = " FOR UPDATE" if for_update else ""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT policy_version,status,effective_from,allowed_days,"
            "about_config,discovery_config,recent_sampling_config,agent_config,"
            "partial_retry_config,checksum "
            "FROM feature_clock.rule_policy_definitions "
            f"ORDER BY effective_from,policy_version{suffix}"
        )
        columns = [column.name for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def inspect_clock_policy_seed(
    connect: Callable[[], Any],
    manifest: ClockPolicyManifest,
    *,
    database: str,
) -> ClockPolicySeedPlan:
    with connect() as connection:
        return plan_clock_policy_seed(
            _load_policy_rows(connection),
            manifest,
            database=database,
        )


def apply_clock_policy_seed(
    connect: Callable[[], Any],
    manifest: ClockPolicyManifest,
    *,
    database: str,
) -> ClockPolicySeedPlan:
    from psycopg.types.json import Jsonb

    with connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("feature-clock-policy-seed-v1",),
            )
        plan = plan_clock_policy_seed(
            _load_policy_rows(connection, for_update=True),
            manifest,
            database=database,
        )
        if plan.action == "already_active":
            return plan
        policy = manifest.policy
        if plan.action == "insert_and_activate":
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO feature_clock.rule_policy_definitions (
                      policy_version,status,effective_from,allowed_days,
                      about_config,discovery_config,recent_sampling_config,
                      agent_config,partial_retry_config,checksum,activated_at
                    ) VALUES (
                      %s,'draft',%s,%s,%s,%s,%s,%s,%s,%s,NULL
                    )
                    """,
                    (
                        policy["policy_version"],
                        policy["effective_from"],
                        policy["allowed_days"],
                        Jsonb(policy["about_config"]),
                        Jsonb(policy["discovery_config"]),
                        Jsonb(policy["recent_sampling_config"]),
                        Jsonb(policy["agent_config"]),
                        Jsonb(policy["partial_retry_config"]),
                        policy["checksum"],
                    ),
                )
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE feature_clock.rule_policy_definitions
                SET status='retired'
                WHERE status='active' AND policy_version<>%s
                """,
                (manifest.policy_version,),
            )
            cursor.execute(
                """
                UPDATE feature_clock.rule_policy_definitions
                SET status='active',activated_at=clock_timestamp()
                WHERE policy_version=%s AND status='draft'
                RETURNING policy_version
                """,
                (manifest.policy_version,),
            )
            if cursor.rowcount != 1:
                raise ClockPolicySeedConflict("Clock policy activation lost its state fence")
    return ClockPolicySeedPlan(
        action="activated",
        database=plan.database,
        policy_version=plan.policy_version,
        previous_active_policy=plan.previous_active_policy,
        source_sha256=plan.source_sha256,
        confirmation=plan.confirmation,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or activate the repository Clock policy manifest."
    )
    parser.add_argument("--policy-file", default=str(DEFAULT_POLICY_FILE))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm")
    return parser


def main() -> None:
    import psycopg

    args = _parser().parse_args()
    manifest = load_clock_policy_manifest(args.policy_file)
    database_url = required_environment(os.environ, "FEATURE_DATABASE_URL")
    expected_database = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE")
    expected_user = required_environment(os.environ, "EXPECTED_FEATURE_DATABASE_USER")
    forbidden_database = str(
        os.environ.get("FORBIDDEN_CRAWLER_DATABASE") or "bullmq_crawler_migration"
    ).strip()

    def connect() -> Any:
        return psycopg.connect(database_url, options="-c timezone=UTC")

    validate_shared_feature_database(
        connect,
        expected_database=expected_database,
        expected_user=expected_user,
        forbidden_database=forbidden_database,
        required_feature_relations=("feature_clock.rule_policy_definitions",),
    )
    plan = inspect_clock_policy_seed(
        connect,
        manifest,
        database=expected_database,
    )
    if not args.execute:
        print(json.dumps({"event": "clock_policy_seed_plan", **plan.as_dict()}))
        return
    if args.confirm != plan.confirmation:
        raise ClockPolicySeedError(
            f"--confirm must equal the plan confirmation: {plan.confirmation}"
        )
    applied = apply_clock_policy_seed(
        connect,
        manifest,
        database=expected_database,
    )
    print(
        json.dumps(
            {
                "event": "clock_policy_seed_applied",
                **applied.as_dict(),
                "domain_rule_versions": manifest.domain_rule_versions,
            }
        )
    )


if __name__ == "__main__":
    main()
