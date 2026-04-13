from __future__ import annotations

from datetime import datetime, timezone

import pytest
from opensearchpy.exceptions import ConflictError

from news_analyzer.domain.enums import ClassLabel, ProcessingStatus, SourceType
from news_analyzer.domain.models import (
    ClassificationResult,
    DedupMetadataUpdate,
    Entity,
    HourlyDigest,
    NormalizedNewsItem,
    SummaryResult,
)
from news_analyzer.storage.opensearch.repositories import HourlyDigestRepository, NewsRepository, ProcessingStateRepository


class _ClientStub:
    def __init__(self) -> None:
        self.index_calls: list[dict[str, object]] = []
        self.update_calls: list[dict[str, object]] = []
        self.search_calls: list[dict[str, object]] = []
        self.exists_calls: list[dict[str, object]] = []
        self.get_calls: list[dict[str, object]] = []

        self.search_response: dict[str, object] = {"hits": {"hits": []}}
        self.exists_response = False
        self.get_response: dict[str, object] = {"_source": {"value": 1}}

    def index(self, **kwargs) -> None:
        self.index_calls.append(kwargs)

    def update(self, **kwargs) -> None:
        self.update_calls.append(kwargs)

    def search(self, **kwargs) -> dict[str, object]:
        self.search_calls.append(kwargs)
        return self.search_response

    def exists(self, **kwargs) -> bool:
        self.exists_calls.append(kwargs)
        return self.exists_response

    def get(self, **kwargs) -> dict[str, object]:
        self.get_calls.append(kwargs)
        return self.get_response


class _ConflictClientStub(_ClientStub):
    def __init__(self, conflict_ids: set[str]) -> None:
        super().__init__()
        self._conflict_ids = conflict_ids

    def index(self, **kwargs) -> None:
        self.index_calls.append(kwargs)
        if kwargs["id"] in self._conflict_ids:
            raise ConflictError(409, "version_conflict_engine_exception", "conflict")


class _InMemoryCreateOnlyClient:
    def __init__(self, existing_docs: dict[str, dict[str, object]] | None = None) -> None:
        self.index_calls: list[dict[str, object]] = []
        self.docs: dict[str, dict[str, object]] = existing_docs or {}

    def index(self, **kwargs) -> None:
        self.index_calls.append(kwargs)
        doc_id = str(kwargs["id"])
        if kwargs.get("op_type") == "create" and doc_id in self.docs:
            raise ConflictError(409, "version_conflict_engine_exception", "conflict")
        self.docs[doc_id] = kwargs["body"]


def _sample_news_item(external_id: str = "id-1", source_type: SourceType = SourceType.RBC) -> NormalizedNewsItem:
    return NormalizedNewsItem(
        source_type=source_type,
        external_id=external_id,
        published_at=datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc),
        source_metadata={"url": f"https://example.com/{external_id}"},
        raw_text="raw",
        cleaned_text="cleaned",
    )


def test_upsert_news_creates_each_item() -> None:
    client = _ClientStub()
    repository = NewsRepository(client=client, index_name="news_items")

    count = repository.upsert_news([_sample_news_item("id-1"), _sample_news_item("id-2")])

    assert count == 2
    assert len(client.index_calls) == 2
    assert client.index_calls[0]["index"] == "news_items"
    assert client.index_calls[0]["id"] == "id-1"
    assert client.index_calls[0]["op_type"] == "create"
    assert client.index_calls[0]["body"]["source_type"] == "rbc"
    assert client.index_calls[0]["body"]["dedup_is_canonical"] is True
    assert client.index_calls[0]["body"]["dedup_canonical_external_id"] == "id-1"
    assert client.index_calls[0]["body"]["dedup_similarity_to_canonical"] == 1.0
    assert "dedup_updated_at" in client.index_calls[0]["body"]


def test_upsert_news_supports_lenta_source_type() -> None:
    client = _ClientStub()
    repository = NewsRepository(client=client, index_name="news_items")

    count = repository.upsert_news([_sample_news_item("id-lenta", source_type=SourceType.LENTA)])

    assert count == 1
    assert client.index_calls[0]["body"]["source_type"] == "lenta"


def test_upsert_news_skips_conflicts_and_counts_only_created() -> None:
    client = _ConflictClientStub(conflict_ids={"id-2"})
    repository = NewsRepository(client=client, index_name="news_items")

    count = repository.upsert_news([_sample_news_item("id-1"), _sample_news_item("id-2")])

    assert count == 1
    assert len(client.index_calls) == 2


def test_upsert_news_reraises_non_conflict_errors() -> None:
    class _ErrorClient(_ClientStub):
        def index(self, **kwargs) -> None:
            self.index_calls.append(kwargs)
            if kwargs["id"] == "id-2":
                raise RuntimeError("unexpected error")

    client = _ErrorClient()
    repository = NewsRepository(client=client, index_name="news_items")

    with pytest.raises(RuntimeError, match="unexpected error"):
        repository.upsert_news([_sample_news_item("id-1"), _sample_news_item("id-2")])


def test_upsert_news_does_not_overwrite_existing_enriched_document() -> None:
    existing_doc = {
        "external_id": "id-1",
        "summary": "old summary",
        "summary_status": "success",
        "class_label": "economy",
        "class_confidence": 0.9,
        "hourly_digest_id": "digest-1",
        "entities": [{"text": "Москва", "label": "LOC"}],
        "dedup_is_canonical": False,
    }
    client = _InMemoryCreateOnlyClient(existing_docs={"id-1": existing_doc.copy()})
    repository = NewsRepository(client=client, index_name="news_items")

    count = repository.upsert_news([_sample_news_item("id-1")])

    assert count == 0
    assert len(client.index_calls) == 1
    assert client.index_calls[0]["op_type"] == "create"
    assert client.docs["id-1"] == existing_doc


def test_set_summary_updates_summary_fields() -> None:
    client = _ClientStub()
    repository = NewsRepository(client=client, index_name="news_items")

    repository.set_summary(
        "id-3",
        SummaryResult(
            summary="summary",
            status=ProcessingStatus.SUCCESS,
            error_code=None,
            updated_at=datetime(2026, 3, 17, 8, 1, tzinfo=timezone.utc),
        ),
    )

    assert len(client.update_calls) == 1
    call = client.update_calls[0]
    assert call["index"] == "news_items"
    assert call["id"] == "id-3"
    assert call["body"]["doc"]["summary"] == "summary"
    assert call["body"]["doc"]["summary_status"] == "success"


def test_set_hourly_digest_link_updates_each_news_item() -> None:
    client = _ClientStub()
    repository = NewsRepository(client=client, index_name="news_items")

    repository.set_hourly_digest_link(["id-1", "id-2"], "digest-1")

    assert len(client.update_calls) == 2
    assert {call["id"] for call in client.update_calls} == {"id-1", "id-2"}
    assert all(call["body"] == {"doc": {"hourly_digest_id": "digest-1"}} for call in client.update_calls)


def test_set_dedup_metadata_bulk_updates_each_news_item() -> None:
    client = _ClientStub()
    repository = NewsRepository(client=client, index_name="news_items")

    repository.set_dedup_metadata_bulk(
        [
            DedupMetadataUpdate(
                external_id="id-1",
                is_canonical=True,
                canonical_external_id="id-1",
                similarity_to_canonical=1.0,
            ),
            DedupMetadataUpdate(
                external_id="id-2",
                is_canonical=False,
                canonical_external_id="id-1",
                similarity_to_canonical=0.95,
            ),
        ],
        updated_at=datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc),
    )

    assert len(client.update_calls) == 2
    first = client.update_calls[0]
    second = client.update_calls[1]
    assert first["id"] == "id-1"
    assert first["body"]["doc"]["dedup_is_canonical"] is True
    assert first["body"]["doc"]["dedup_canonical_external_id"] == "id-1"
    assert first["body"]["doc"]["dedup_similarity_to_canonical"] == 1.0
    assert second["id"] == "id-2"
    assert second["body"]["doc"]["dedup_is_canonical"] is False
    assert second["body"]["doc"]["dedup_canonical_external_id"] == "id-1"
    assert second["body"]["doc"]["dedup_similarity_to_canonical"] == 0.95


def test_set_enrichment_writes_entities_and_classification() -> None:
    client = _ClientStub()
    repository = NewsRepository(client=client, index_name="news_items")

    repository.set_enrichment(
        "id-11",
        entities=[Entity(text="Москва", label="LOC", start=0, end=6, confidence=0.9, normalized="москва")],
        classification=ClassificationResult(class_label=ClassLabel.ECONOMY, class_confidence=0.7, model_version="v1"),
    )

    assert len(client.update_calls) == 1
    body = client.update_calls[0]["body"]["doc"]
    assert body["entities"][0]["normalized"] == "москва"
    assert body["class_label"] == "economy"
    assert body["class_confidence"] == 0.7


def test_get_recent_news_without_summary_builds_query_and_maps_hits(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.datetime", _FakeDatetime)

    client = _ClientStub()
    client.search_response = {
        "hits": {
            "hits": [
                {"_id": "id-1", "_source": {"cleaned_text": "text-1"}},
                {"_id": "id-2", "_source": {"cleaned_text": "text-2"}},
            ]
        }
    }
    repository = NewsRepository(client=client, index_name="news_items")

    items = repository.get_recent_news_without_summary(limit=7)

    assert [item["external_id"] for item in items] == ["id-1", "id-2"]
    call = client.search_calls[0]
    assert call["index"] == "news_items"
    assert call["body"]["size"] == 7
    assert call["body"]["query"]["bool"]["must_not"] == [{"exists": {"field": "summary"}}]


def test_get_recent_canonical_news_without_summary_builds_query(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.datetime", _FakeDatetime)

    client = _ClientStub()
    client.search_response = {"hits": {"hits": []}}
    repository = NewsRepository(client=client, index_name="news_items")

    repository.get_recent_canonical_news_without_summary(limit=5)

    call = client.search_calls[0]["body"]
    assert call["size"] == 5
    assert call["query"]["bool"]["must_not"] == [{"exists": {"field": "summary"}}]
    canonical_clause = call["query"]["bool"]["must"][1]
    assert canonical_clause["bool"]["minimum_should_match"] == 1
    assert canonical_clause["bool"]["should"][0] == {"term": {"dedup_is_canonical": True}}


def test_get_recent_news_without_enrichment_builds_query_and_maps_hits(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.datetime", _FakeDatetime)

    client = _ClientStub()
    client.search_response = {
        "hits": {
            "hits": [
                {"_id": "id-7", "_source": {"cleaned_text": "text-7"}},
            ]
        }
    }
    repository = NewsRepository(client=client, index_name="news_items")

    items = repository.get_recent_news_without_enrichment(limit=3, hours=12)

    assert items[0]["external_id"] == "id-7"
    call = client.search_calls[0]
    assert call["body"]["size"] == 3
    assert call["body"]["query"]["bool"]["must_not"] == [{"exists": {"field": "entities"}}]


def test_get_news_for_last_hours_and_last_hour_queries(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.datetime", _FakeDatetime)

    client = _ClientStub()
    client.search_response = {
        "hits": {"hits": [{"_id": "id-9", "_source": {"value": 1}}]}
    }
    repository = NewsRepository(client=client, index_name="news_items")

    rows = repository.get_news_for_last_hours(hours=6, limit=11)
    rows_last_hour = repository.get_news_for_last_hour(limit=2)

    assert rows[0]["external_id"] == "id-9"
    assert rows_last_hour[0]["external_id"] == "id-9"
    first_call = client.search_calls[0]["body"]
    second_call = client.search_calls[1]["body"]
    assert first_call["size"] == 11
    assert second_call["size"] == 2
    assert first_call["sort"] == [{"published_at": {"order": "asc"}}]


def test_get_canonical_news_for_last_hour_adds_canonical_clause(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.datetime", _FakeDatetime)

    client = _ClientStub()
    client.search_response = {"hits": {"hits": []}}
    repository = NewsRepository(client=client, index_name="news_items")

    repository.get_canonical_news_for_last_hour(limit=4)

    body = client.search_calls[0]["body"]
    assert body["size"] == 4
    query = body["query"]
    assert query["bool"]["must"][1]["bool"]["should"][0] == {"term": {"dedup_is_canonical": True}}


def test_get_news_for_dedup_candidates_builds_window_query(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    monkeypatch.setattr("news_analyzer.storage.opensearch.repositories.datetime", _FakeDatetime)

    client = _ClientStub()
    client.search_response = {"hits": {"hits": [{"_id": "id-1", "_source": {"published_at": "2026-03-17T07:50:00+00:00"}}]}}
    repository = NewsRepository(client=client, index_name="news_items")

    rows = repository.get_news_for_dedup_candidates(lookback_hours=24, limit=50)

    assert rows[0]["external_id"] == "id-1"
    body = client.search_calls[0]["body"]
    assert body["size"] == 50
    assert body["sort"] == [{"published_at": {"order": "asc"}}, {"external_id": {"order": "asc"}}]


def test_hourly_digest_repository_upsert_indexes_digest() -> None:
    client = _ClientStub()
    repository = HourlyDigestRepository(client=client, index_name="hourly_digests")

    repository.upsert(
        HourlyDigest(
            digest_id="digest-1",
            window_start=datetime(2026, 3, 17, 7, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc),
            summary="summary",
            news_ids=["id-1", "id-2"],
        )
    )

    assert len(client.index_calls) == 1
    call = client.index_calls[0]
    assert call["index"] == "hourly_digests"
    assert call["id"] == "digest-1"
    assert call["body"]["news_ids"] == ["id-1", "id-2"]


def test_processing_state_repository_put_and_get_state() -> None:
    client = _ClientStub()
    repository = ProcessingStateRepository(client=client, index_name="processing_state")

    repository.put_state("state-1", {"offset": 100})

    assert client.index_calls[0] == {"index": "processing_state", "id": "state-1", "body": {"offset": 100}}

    client.exists_response = False
    assert repository.get_state("state-1") is None

    client.exists_response = True
    client.get_response = {"_source": {"offset": 200}}
    assert repository.get_state("state-1") == {"offset": 200}
