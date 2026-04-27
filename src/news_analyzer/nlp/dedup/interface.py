from __future__ import annotations

from typing import Protocol


class TextEmbeddingModel(Protocol):
    def embed(self, texts: list[str]) -> list[list[float]]:
        ...
