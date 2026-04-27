from __future__ import annotations

import json

from news_analyzer.storage.opensearch.indices import OpenSearchIndexManager


class _IndicesStub:
    def __init__(self, exists_value: bool) -> None:
        self.exists_value = exists_value
        self.exists_calls: list[str] = []
        self.create_calls: list[dict[str, object]] = []

    def exists(self, index: str) -> bool:
        self.exists_calls.append(index)
        return self.exists_value

    def create(self, index: str, body: dict[str, object]) -> None:
        self.create_calls.append({"index": index, "body": body})


class _ClientStub:
    def __init__(self, exists_value: bool) -> None:
        self.indices = _IndicesStub(exists_value=exists_value)


def test_ensure_skips_create_when_index_exists() -> None:
    client = _ClientStub(exists_value=True)
    manager = OpenSearchIndexManager(client)

    manager.ensure("news_items", "news.json")

    assert client.indices.exists_calls == ["news_items"]
    assert client.indices.create_calls == []


def test_ensure_reads_mapping_and_creates_index_when_missing(monkeypatch) -> None:
    client = _ClientStub(exists_value=False)
    manager = OpenSearchIndexManager(client)

    monkeypatch.setattr(
        "news_analyzer.storage.opensearch.indices.Path.read_text",
        lambda self, encoding="utf-8": json.dumps({"mappings": {"properties": {"field": {"type": "keyword"}}}}),
    )

    manager.ensure("hourly_digests", "hourly_digests.json")

    assert client.indices.exists_calls == ["hourly_digests"]
    assert len(client.indices.create_calls) == 1
    assert client.indices.create_calls[0]["index"] == "hourly_digests"
    assert client.indices.create_calls[0]["body"] == {"mappings": {"properties": {"field": {"type": "keyword"}}}}
