from __future__ import annotations

from typing import Protocol

from news_analyzer.domain.models import Entity


class NERModel(Protocol):
    def extract(self, text: str) -> list[Entity]:
        ...
