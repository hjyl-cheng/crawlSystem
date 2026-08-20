from __future__ import annotations

from copy import deepcopy
from typing import Any, Iterable, Sequence

from .contracts import AGE_RANGES, FACT_FIELDS, ProfileAnalysisResult
from .errors import ContractError
from .taxonomy import valid_categories
from .analyzers import controlled_tag_names


AGENT_OUTPUT_FIELDS = ("input_url", *FACT_FIELDS)
_FORBIDDEN_TEXT = {"", "unknown", "null", "n/a", "unavailable", "uncertain"}
_INVALID_COUNTRIES = {
    "global",
    "worldwide",
    "multiple countries",
    "other",
    "africa",
    "antarctica",
    "asia",
    "europe",
    "north america",
    "oceania",
    "south america",
}
_GENERIC_TAGS = {
    "content",
    "videos",
    "youtube",
    "entertainment",
    "creator",
    "interesting",
    "popular",
    "social media",
}


def _fail(path: str, message: str) -> None:
    raise ContractError(f"Agent output {path} {message}")


def _exact_keys(value: Any, keys: Sequence[str], path: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        _fail(path, "must be an object")
    if tuple(value) != tuple(keys):
        _fail(path, f"must contain exactly these fields in order: {', '.join(keys)}")
    return value


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str) or value.strip().casefold() in _FORBIDDEN_TEXT:
        _fail(path, "must be a specific non-empty string")
    return value


def _integer(value: Any, path: str, *, minimum: int = 0, maximum: int = 100) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        _fail(path, "must be an integer")
    if not minimum <= value <= maximum:
        _fail(path, f"must be in [{minimum}, {maximum}]")
    return value


def _percentage_rows(
    value: Any,
    *,
    path: str,
    label_key: str,
    exact_count: int | None = None,
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        _fail(path, "must be a non-empty array")
    if exact_count is not None and len(value) != exact_count:
        _fail(path, f"must contain exactly {exact_count} items")
    labels: list[str] = []
    for index, raw in enumerate(value):
        row = _exact_keys(raw, (label_key, "percentage"), f"{path}[{index}]")
        labels.append(_text(row[label_key], f"{path}[{index}].{label_key}"))
        _integer(row["percentage"], f"{path}[{index}].percentage")
    if len({label.casefold() for label in labels}) != len(labels):
        _fail(path, f"contains duplicate {label_key} values")
    if sum(row["percentage"] for row in value) != 100:
        _fail(path, "percentages must sum to exactly 100")
    return value


def validate_agent_payload(payload: Any) -> None:
    """Validate one raw object against the current Agent prompt contract."""

    value = _exact_keys(payload, AGENT_OUTPUT_FIELDS, "object")
    _text(value["input_url"], "input_url")

    country = _text(value["country"], "country")
    if country.strip().casefold() in _INVALID_COUNTRIES:
        _fail("country", "must be a specific country or region, not a global/continent label")

    if value["creator_gender"] not in {"male", "female", "brand_team"}:
        _fail("creator_gender", "must be male, female, or brand_team")
    _integer(value["creator_age_range"], "creator_age_range", maximum=120)
    _text(value["creator_language"], "creator_language")

    regions = _percentage_rows(
        value["audience_region"],
        path="audience_region",
        label_key="region",
        exact_count=6,
    )
    if regions[-1]["region"] != "Other":
        _fail("audience_region[5].region", 'must be the literal "Other"')
    for index, row in enumerate(regions[:5]):
        if row["region"].strip().casefold() in _INVALID_COUNTRIES:
            _fail(f"audience_region[{index}].region", "must be a specific country or region")
    if any(
        regions[index]["percentage"] < regions[index + 1]["percentage"]
        for index in range(4)
    ):
        _fail("audience_region", "first five items must be ordered by descending percentage")

    age_gender = value["audience_age_gender"]
    if not isinstance(age_gender, list) or len(age_gender) != len(AGE_RANGES):
        _fail("audience_age_gender", "must contain exactly the six fixed age groups")
    age_gender_total = 0
    for index, (raw, expected_age) in enumerate(zip(age_gender, AGE_RANGES)):
        row = _exact_keys(raw, ("age_range", "male", "female"), f"audience_age_gender[{index}]")
        if row["age_range"] != expected_age:
            _fail(f"audience_age_gender[{index}].age_range", f"must be {expected_age}")
        age_gender_total += _integer(row["male"], f"audience_age_gender[{index}].male")
        age_gender_total += _integer(row["female"], f"audience_age_gender[{index}].female")
    if age_gender_total != 100:
        _fail("audience_age_gender", "12 percentages must sum to exactly 100")

    languages = _percentage_rows(
        value["audience_language"],
        path="audience_language",
        label_key="language",
    )
    other_indices = [index for index, row in enumerate(languages) if row["language"] == "Other"]
    if other_indices and other_indices != [len(languages) - 1]:
        _fail("audience_language", 'the optional "Other" item must be last')
    if any(
        languages[index]["percentage"] < languages[index + 1]["percentage"]
        for index in range(len(languages) - 1)
    ):
        _fail("audience_language", "must be ordered by descending percentage")

    _integer(value["active_subscriber_ratio"], "active_subscriber_ratio")

    tags = _exact_keys(value["channel_tags"], ("tags", "top_5_distribution"), "channel_tags")
    names = tags["tags"]
    if not isinstance(names, list) or len(names) != 10:
        _fail("channel_tags.tags", "must contain exactly 10 items")
    checked_names = [_text(name, f"channel_tags.tags[{index}]") for index, name in enumerate(names)]
    if len({name.casefold() for name in checked_names}) != 10:
        _fail("channel_tags.tags", "must contain 10 unique tags")
    for index, name in enumerate(checked_names):
        if name.strip().casefold() in _GENERIC_TAGS:
            _fail(f"channel_tags.tags[{index}]", "is prohibited because it is overly generic")
    controlled = {name.casefold() for name in controlled_tag_names()}
    if any(name.casefold() not in controlled for name in checked_names):
        _fail("channel_tags.tags", "must use the versioned English controlled vocabulary")
    tag_distribution = _percentage_rows(
        tags["top_5_distribution"],
        path="channel_tags.top_5_distribution",
        label_key="tag",
        exact_count=6,
    )
    if [row["tag"] for row in tag_distribution[:5]] != checked_names[:5]:
        _fail("channel_tags.top_5_distribution", "must exactly match the first five tags in order")
    if tag_distribution[-1]["tag"] != "Other":
        _fail("channel_tags.top_5_distribution[5].tag", 'must be the literal "Other"')
    if any(
        tag_distribution[index]["percentage"] < tag_distribution[index + 1]["percentage"]
        for index in range(4)
    ):
        _fail("channel_tags.top_5_distribution", "first five items must be ordered by descending percentage")

    categories = _exact_keys(
        value["channel_categories"],
        ("level_1", "level_2"),
        "channel_categories",
    )
    if not valid_categories(categories):
        _fail("channel_categories", "must use one valid taxonomy branch and 1-3 unique level_2 values")


def _ordered_payload(input_url: Any, values: dict[str, Any]) -> dict[str, Any]:
    regions = values["audience_region"]
    age_gender = values["audience_age_gender"]
    languages = values["audience_language"]
    tags = values["channel_tags"]
    categories = values["channel_categories"]
    return {
        "input_url": input_url,
        "country": values["country"],
        "creator_gender": values["creator_gender"],
        "creator_age_range": values["creator_age_range"],
        "creator_language": values["creator_language"],
        "audience_region": [
            {"region": row["region"], "percentage": row["percentage"]}
            for row in regions
        ],
        "audience_age_gender": [
            {"age_range": row["age_range"], "male": row["male"], "female": row["female"]}
            for row in age_gender
        ],
        "audience_language": [
            {"language": row["language"], "percentage": row["percentage"]}
            for row in languages
        ],
        "active_subscriber_ratio": values["active_subscriber_ratio"],
        "channel_tags": {
            "tags": list(tags["tags"]),
            "top_5_distribution": [
                {"tag": row["tag"], "percentage": row["percentage"]}
                for row in tags["top_5_distribution"]
            ],
        },
        "channel_categories": {
            "level_1": categories["level_1"],
            "level_2": list(categories["level_2"]),
        },
    }


def to_agent_payload(result: ProfileAnalysisResult) -> dict[str, Any]:
    """Project an internal result onto the strict legacy 11-field response shape."""

    result.validate()
    facts = result.facts
    unavailable = [name for name in FACT_FIELDS if facts[name].value is None or facts[name].abstained]
    if unavailable:
        raise ContractError(
            "Agent-compatible output requires complete_estimate values; unavailable fields: "
            + ", ".join(unavailable)
        )
    payload = _ordered_payload(
        result.input_url,
        {name: deepcopy(facts[name].value) for name in FACT_FIELDS},
    )
    validate_agent_payload(payload)
    return payload


def internal_result_to_agent_payload(result: Any) -> dict[str, Any]:
    """Project a serialized internal result without rerunning inference."""

    if not isinstance(result, dict):
        raise ContractError("serialized internal result must be an object")
    facts = result.get("facts")
    if not isinstance(facts, dict) or set(facts) != set(FACT_FIELDS):
        raise ContractError("serialized internal result must contain exactly the ten fact fields")
    values: dict[str, Any] = {}
    unavailable: list[str] = []
    for name in FACT_FIELDS:
        field = facts.get(name)
        if not isinstance(field, dict) or "value" not in field:
            raise ContractError(f"serialized internal result is missing facts.{name}.value")
        if field["value"] is None or field.get("abstained") is True:
            unavailable.append(name)
        values[name] = deepcopy(field["value"])
    if unavailable:
        raise ContractError(
            "Agent-compatible output requires complete values; unavailable fields: "
            + ", ".join(unavailable)
        )
    payload = _ordered_payload(result.get("input_url"), values)
    validate_agent_payload(payload)
    return payload


def validate_agent_batch(
    payloads: Iterable[dict[str, Any]],
    *,
    expected_input_urls: Sequence[str] | None = None,
) -> int:
    """Validate cardinality, order, duplicates, and every per-object field rule."""

    rows = list(payloads)
    for payload in rows:
        validate_agent_payload(payload)
    if expected_input_urls is not None:
        actual = [row["input_url"] for row in rows]
        if actual != list(expected_input_urls):
            raise ContractError(
                "Agent output input_url values must match the input array character-for-character and in order"
            )
    return len(rows)
