from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class RBCCollectorConfig:
    sections: list[str]
    request_timeout: int = 20
    pages_limit: int = 2
    max_retries: int = 3
    backoff_seconds: float = 0.5
    fallback_enabled: bool = True
    user_agent: str | None = None

    @classmethod
    def from_sources_file(cls, path: Path) -> "RBCCollectorConfig":
        with path.open("r", encoding="utf-8") as handle:
            data: dict[str, Any] = yaml.safe_load(handle) or {}

        rbc = data.get("rbc", {})
        sections = rbc.get("sections", ["economics"])
        if not isinstance(sections, list):
            raise ValueError("rbc.sections must be a list")

        return cls(
            sections=[str(value).strip() for value in sections if str(value).strip()],
            request_timeout=int(rbc.get("request_timeout", 20)),
            pages_limit=int(rbc.get("pages_limit", 2)),
            max_retries=int(rbc.get("max_retries", 3)),
            backoff_seconds=float(rbc.get("backoff_seconds", 0.5)),
            fallback_enabled=_to_bool(rbc.get("fallback_enabled", True)),
            user_agent=str(rbc.get("user_agent")).strip() if rbc.get("user_agent") else None,
        )


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return bool(value)
