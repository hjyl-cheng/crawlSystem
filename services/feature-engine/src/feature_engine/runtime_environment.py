from __future__ import annotations

from pathlib import Path
from typing import Mapping


def environment_value(
    environment: Mapping[str, str],
    name: str,
    *,
    required: bool = True,
) -> str | None:
    direct = str(environment.get(name) or "").strip()
    file_path = str(environment.get(f"{name}_FILE") or "").strip()
    if direct and file_path:
        raise RuntimeError(f"{name} and {name}_FILE cannot both be set")
    if file_path:
        try:
            direct = Path(file_path).read_text(encoding="utf-8").strip()
        except OSError as error:
            raise RuntimeError(f"cannot read {name}_FILE") from error
    if required and not direct:
        raise RuntimeError(f"{name} or {name}_FILE is required")
    return direct or None


def required_environment(environment: Mapping[str, str], name: str) -> str:
    value = environment_value(environment, name)
    assert value is not None
    return value


def optional_environment(environment: Mapping[str, str], name: str) -> str | None:
    return environment_value(environment, name, required=False)
