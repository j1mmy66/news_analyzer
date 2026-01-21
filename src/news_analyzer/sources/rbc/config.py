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
        )
