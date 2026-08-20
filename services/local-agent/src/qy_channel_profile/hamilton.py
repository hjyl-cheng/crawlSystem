from __future__ import annotations

import math
from collections.abc import Mapping, Sequence

from .errors import ContractError


def apportion(
    weights: Mapping[str, float],
    *,
    total: int = 100,
    order: Sequence[str] | None = None,
) -> dict[str, int]:
    """Convert non-negative weights to integers with a deterministic exact sum."""

    if total < 0:
        raise ContractError("Hamilton target total must be non-negative")
    keys = list(order) if order is not None else list(weights)
    if not keys or len(set(keys)) != len(keys) or set(keys) != set(weights):
        raise ContractError("Hamilton keys/order must be non-empty and identical")
    values: dict[str, float] = {}
    for key in keys:
        value = float(weights[key])
        if not math.isfinite(value) or value < 0:
            raise ContractError(f"Hamilton weight for {key!r} must be finite and non-negative")
        values[key] = value
    weight_total = sum(values.values())
    if weight_total <= 0:
        raise ContractError("Hamilton cannot apportion an all-zero distribution")

    quotas = {key: values[key] * total / weight_total for key in keys}
    result = {key: math.floor(quotas[key]) for key in keys}
    shortfall = total - sum(result.values())
    tie_order = {key: index for index, key in enumerate(keys)}
    ranked = sorted(
        keys,
        key=lambda key: (-(quotas[key] - result[key]), tie_order[key]),
    )
    for key in ranked[:shortfall]:
        result[key] += 1
    if sum(result.values()) != total:
        raise ContractError("Hamilton postcondition failed")
    return result

