from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class LentaCollectorConfig:
    rss_url: str = "https://lenta.ru/rss/news"
    request_timeout: int = 20
    max_retries: int = 3
    backoff_seconds: float = 0.5
    user_agent: str | None = None
    items_limit: int = 100

    @classmethod
    def from_sources_file(cls, path: Path) -> "LentaCollectorConfig":
        with path.open("r", encoding="utf-8") as handle:
            data: dict[str, Any] = yaml.safe_load(handle) or {}

        lenta = data.get("lenta", {})
        return cls(
            rss_url=str(lenta.get("rss_url", "https://lenta.ru/rss/news")).strip() or "https://lenta.ru/rss/news",
            request_timeout=int(lenta.get("request_timeout", 20)),
            max_retries=int(lenta.get("max_retries", 3)),
            backoff_seconds=float(lenta.get("backoff_seconds", 0.5)),
            user_agent=str(lenta.get("user_agent")).strip() if lenta.get("user_agent") else None,
            items_limit=max(1, int(lenta.get("items_limit", 100))),
        )
