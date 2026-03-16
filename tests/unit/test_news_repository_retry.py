from __future__ import annotations

import pytest
from opensearchpy.exceptions import ConflictError

from news_analyzer.domain.enums import ClassLabel
from news_analyzer.domain.models import ClassificationResult, Entity
from news_analyzer.storage.opensearch.repositories import NewsRepository


class _ClientStub:
    def __init__(self, failures_before_success: int) -> None:
        self.failures_before_success = failures_before_success
        self.calls = 0

    def update(self, **kwargs) -> None:
        self.calls += 1
        if self.calls <= self.failures_before_success:
            raise ConflictError(409, "version_conflict_engine_exception", "conflict")


def _sample_entities() -> list[Entity]:
    return [Entity(text="Москва", label="LOC", start=0, end=6, confidence=0.9, normalized="москва")]


def _sample_classification() -> ClassificationResult:
    return ClassificationResult(class_label=ClassLabel.POLITICS, class_confidence=0.7, model_version="test")


def test_set_enrichment_retries_conflict_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _ClientStub(failures_before_success=2)
    repository = NewsRepository(client=client, index_name="news_items")
    delays: list[float] = []
    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.time.sleep", lambda value: delays.append(value))

    repository.set_enrichment("id-1", _sample_entities(), _sample_classification())

    assert client.calls == 3
    assert delays == [0.1, 0.2]


def test_set_enrichment_raises_after_max_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _ClientStub(failures_before_success=10)
    repository = NewsRepository(client=client, index_name="news_items")
    delays: list[float] = []
    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.time.sleep", lambda value: delays.append(value))

    with pytest.raises(ConflictError):
        repository.set_enrichment("id-2", _sample_entities(), _sample_classification())

    assert client.calls == 3
    assert delays == [0.1, 0.2]

