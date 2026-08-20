from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .taxonomy import CHANNEL_CATEGORY_TREE


TAXONOMY_V2_DRAFT_VERSION = "qy-taxonomy-v2-draft.1"
TAXONOMY_V2_DRAFT_PATH = Path(__file__).with_name("catalogs") / "qy_taxonomy_v2_draft.json"
REQUIRED_FACETS = frozenset({"topic", "purpose_genre", "format", "source", "entity"})
MIGRATION_METHODS = frozenset({
    "exact",
    "facet_split",
    "parent_fallback",
    "manual_review",
    "no_equivalent",
})


class TaxonomyDraftError(ValueError):
    pass


def _legacy_keys() -> set[tuple[str, str | None]]:
    keys: set[tuple[str, str | None]] = set()
    for level_1, children in CHANNEL_CATEGORY_TREE.items():
        keys.add((level_1, None))
        keys.update((level_1, child) for child in children)
    return keys


def _validate_mapping_decision(
    decision: Any,
    *,
    key: tuple[str, str | None],
    node_ids: set[str],
) -> None:
    if not isinstance(decision, dict):
        raise TaxonomyDraftError(f"legacy mapping {key} must be an object")
    method = decision.get("method")
    targets = decision.get("targets")
    if method not in MIGRATION_METHODS or not isinstance(targets, list):
        raise TaxonomyDraftError(f"legacy mapping {key} has an invalid method or targets")
    unknown_targets = set(targets).difference(node_ids)
    if unknown_targets:
        raise TaxonomyDraftError(f"legacy mapping {key} has unknown targets: {sorted(unknown_targets)}")
    if method not in {"manual_review", "no_equivalent"} and not targets:
        raise TaxonomyDraftError(f"legacy mapping {key} requires a target")
    if not str(decision.get("note") or "").strip():
        raise TaxonomyDraftError(f"legacy mapping {key} has no rationale")


def expanded_legacy_mappings(payload: dict[str, Any]) -> dict[tuple[str, str | None], dict[str, Any]]:
    compatibility = payload.get("legacy_v1_compatibility")
    branches = compatibility.get("branches") if isinstance(compatibility, dict) else None
    if not isinstance(branches, list):
        raise TaxonomyDraftError("legacy v1 compatibility branches are missing")
    expanded: dict[tuple[str, str | None], dict[str, Any]] = {}
    for branch in branches:
        if not isinstance(branch, dict):
            raise TaxonomyDraftError("legacy compatibility branch must be an object")
        level_1 = branch.get("legacy_level_1")
        if level_1 not in CHANNEL_CATEGORY_TREE:
            raise TaxonomyDraftError(f"unknown legacy level 1 branch: {level_1}")
        overrides = branch.get("overrides", {})
        if not isinstance(overrides, dict):
            raise TaxonomyDraftError(f"legacy branch {level_1} overrides must be an object")
        unknown_children = set(overrides).difference(CHANNEL_CATEGORY_TREE[level_1])
        if unknown_children:
            raise TaxonomyDraftError(
                f"legacy branch {level_1} has unknown overrides: {sorted(unknown_children)}"
            )
        parent_key = (level_1, None)
        if parent_key in expanded:
            raise TaxonomyDraftError(f"duplicate legacy branch: {level_1}")
        expanded[parent_key] = branch.get("parent")
        default_child = branch.get("default_child")
        for child in CHANNEL_CATEGORY_TREE[level_1]:
            expanded[(level_1, child)] = overrides.get(child, default_child)
    return expanded


def _validate_parent_graph(nodes: dict[str, tuple[str, dict[str, Any]]]) -> None:
    for node_id, (facet_name, node) in nodes.items():
        parent_id = node.get("parent_id")
        if parent_id is None:
            continue
        parent = nodes.get(parent_id)
        if parent is None:
            raise TaxonomyDraftError(f"node {node_id} has unknown parent {parent_id}")
        if parent[0] != facet_name:
            raise TaxonomyDraftError(f"node {node_id} has a parent in another facet")
        visited = {node_id}
        cursor = parent_id
        while cursor is not None:
            if cursor in visited:
                raise TaxonomyDraftError(f"taxonomy cycle detected at {node_id}")
            visited.add(cursor)
            cursor = nodes[cursor][1].get("parent_id")


def validate_taxonomy_v2_draft(payload: dict[str, Any]) -> None:
    if payload.get("version") != TAXONOMY_V2_DRAFT_VERSION:
        raise TaxonomyDraftError("unexpected taxonomy v2 draft version")
    if payload.get("status") != "draft_shadow_only" or payload.get("production_eligible") is not False:
        raise TaxonomyDraftError("taxonomy v2 must remain an ineligible shadow draft")
    facets = payload.get("facets")
    if not isinstance(facets, dict) or set(facets) != REQUIRED_FACETS:
        raise TaxonomyDraftError("taxonomy v2 facets are incomplete")

    nodes: dict[str, tuple[str, dict[str, Any]]] = {}
    for facet_name, facet in facets.items():
        if not isinstance(facet, dict) or not str(facet.get("definition") or "").strip():
            raise TaxonomyDraftError(f"facet {facet_name} has no definition")
        rows = facet.get("nodes")
        if not isinstance(rows, list) or not rows:
            raise TaxonomyDraftError(f"facet {facet_name} has no nodes")
        labels: set[str] = set()
        for node in rows:
            if not isinstance(node, dict):
                raise TaxonomyDraftError(f"facet {facet_name} contains a non-object node")
            node_id = str(node.get("id") or "").strip()
            label = str(node.get("label_en") or "").strip()
            if not node_id.startswith("qy.") or not label:
                raise TaxonomyDraftError(f"facet {facet_name} has an invalid node identity")
            if node_id in nodes:
                raise TaxonomyDraftError(f"duplicate taxonomy node ID: {node_id}")
            if label.casefold() in labels:
                raise TaxonomyDraftError(f"duplicate label in facet {facet_name}: {label}")
            if not str(node.get("definition") or "").strip():
                raise TaxonomyDraftError(f"node {node_id} has no definition")
            for boundary in ("includes", "excludes"):
                values = node.get(boundary)
                if not isinstance(values, list) or not values or not all(
                    isinstance(value, str) and value.strip() for value in values
                ):
                    raise TaxonomyDraftError(f"node {node_id} has invalid {boundary}")
            labels.add(label.casefold())
            nodes[node_id] = (facet_name, node)
    _validate_parent_graph(nodes)

    compatibility = payload.get("legacy_v1_compatibility")
    if not isinstance(compatibility, dict) or compatibility.get("compatibility_only") is not True:
        raise TaxonomyDraftError("legacy v1 mapping must be compatibility-only")
    mappings = expanded_legacy_mappings(payload)
    seen = set(mappings)
    for key, decision in mappings.items():
        _validate_mapping_decision(decision, key=key, node_ids=set(nodes))
    expected = _legacy_keys()
    if seen != expected:
        missing = sorted(expected.difference(seen), key=str)
        extra = sorted(seen.difference(expected), key=str)
        raise TaxonomyDraftError(f"legacy mapping coverage mismatch; missing={missing}; extra={extra}")

    projection = compatibility.get("v2_to_v1_projection")
    if not isinstance(projection, list):
        raise TaxonomyDraftError("v2 to v1 compatibility projection is missing")
    topic_roots = {
        node_id
        for node_id, (facet_name, node) in nodes.items()
        if facet_name == "topic" and node.get("parent_id") is None
    }
    projected_roots: set[str] = set()
    for rule in projection:
        if not isinstance(rule, dict):
            raise TaxonomyDraftError("v2 to v1 projection rule must be an object")
        root_id = rule.get("topic_root_id")
        level_1 = rule.get("legacy_level_1")
        level_2 = rule.get("default_level_2")
        if root_id in projected_roots:
            raise TaxonomyDraftError(f"duplicate v2 projection root: {root_id}")
        if root_id not in topic_roots:
            raise TaxonomyDraftError(f"v2 projection references a non-root topic: {root_id}")
        if level_1 not in CHANNEL_CATEGORY_TREE or level_2 not in CHANNEL_CATEGORY_TREE[level_1]:
            raise TaxonomyDraftError(f"v2 projection has an invalid legacy target: {(level_1, level_2)}")
        if not str(rule.get("note") or "").strip():
            raise TaxonomyDraftError(f"v2 projection {root_id} has no information-loss note")
        projected_roots.add(root_id)
    if projected_roots != topic_roots:
        raise TaxonomyDraftError(
            f"v2 topic projection coverage mismatch: {sorted(topic_roots.difference(projected_roots))}"
        )


def load_taxonomy_v2_draft(path: str | Path | None = None) -> dict[str, Any]:
    source = Path(path).resolve() if path is not None else TAXONOMY_V2_DRAFT_PATH
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise TaxonomyDraftError(f"cannot load taxonomy v2 draft: {error}") from error
    if not isinstance(payload, dict):
        raise TaxonomyDraftError("taxonomy v2 draft must be a JSON object")
    validate_taxonomy_v2_draft(payload)
    return payload
