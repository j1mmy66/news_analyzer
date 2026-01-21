from __future__ import annotations

from typing import Protocol

from news_analyzer.domain.models import ClassificationResult


class ClassificationModel(Protocol):
    def classify(self, text: str) -> ClassificationResult:
        ...
