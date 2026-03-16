from __future__ import annotations

import hashlib
from dataclasses import dataclass


@dataclass
class InMemorySummaryCache:
    _cache: dict[str, str]

    def __init__(self) -> None:
        self._cache = {}

    def build_key(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def get(self, text: str) -> str | None:
        return self._cache.get(self.build_key(text))

    def set(self, text: str, summary: str) -> None:
        self._cache[self.build_key(text)] = summary
