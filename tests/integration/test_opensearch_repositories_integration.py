from __future__ import annotations

from datetime import datetime, timedelta, timezone

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


def _news_item(external_id: str, *, published_at: datetime, cleaned_text: str = "cleaned") -> NormalizedNewsItem:
    return NormalizedNewsItem(
        source_type=SourceType.RBC,
        external_id=external_id,
        published_at=published_at,
        source_metadata={"url": f"https://example.com/{external_id}"},
        raw_text=f"raw-{external_id}",
        cleaned_text=cleaned_text,
    )


def _success_classification() -> ClassificationResult:
    return ClassificationResult(
        class_label=ClassLabel.ECONOMY,
        class_confidence=0.77,
        model_version="it",
    )


def test_news_repository_upsert_conflict_preserves_existing_document(opensearch_client, indexed_os_names) -> None:
    index_name = indexed_os_names["news_index"]
    repository = NewsRepository(opensearch_client, index_name)
    now = datetime.now(timezone.utc)

    created = repository.upsert_news([_news_item("doc-1", published_at=now, cleaned_text="clean-one")])
    assert created == 1

    repository.set_enrichment(
        "doc-1",
        entities=[Entity(text="Москва", label="LOC", start=0, end=6, confidence=0.9, normalized="москва")],
        classification=_success_classification(),
    )
    repository.set_summary(
        "doc-1",
        SummaryResult(
            summary="sum-1",
            status=ProcessingStatus.SUCCESS,
            error_code=None,
            updated_at=now,
        ),
    )
    repository.set_dedup_metadata_bulk(
        [
            DedupMetadataUpdate(
                external_id="doc-1",
                is_canonical=False,
                canonical_external_id="doc-root",
                similarity_to_canonical=0.91,
            )
        ],
        updated_at=now,
    )
    opensearch_client.indices.refresh(index=index_name)

    created_again = repository.upsert_news([_news_item("doc-1", published_at=now, cleaned_text="clean-two")])
    assert created_again == 0
    opensearch_client.indices.refresh(index=index_name)

    payload = opensearch_client.get(index=index_name, id="doc-1")["_source"]
    assert payload["cleaned_text"] == "clean-one"
    assert payload["summary"] == "sum-1"
    assert payload["summary_status"] == "success"
    assert payload["class_label"] == "economy"
    assert payload["dedup_is_canonical"] is False
    assert payload["dedup_canonical_external_id"] == "doc-root"


def test_news_repository_setters_persist_expected_fields(opensearch_client, indexed_os_names) -> None:
    index_name = indexed_os_names["news_index"]
    repository = NewsRepository(opensearch_client, index_name)
    now = datetime.now(timezone.utc)

    created = repository.upsert_news(
        [
            _news_item("doc-a", published_at=now - timedelta(minutes=10)),
            _news_item("doc-b", published_at=now - timedelta(minutes=9)),
        ]
    )
    assert created == 2

    repository.set_enrichment(
        "doc-a",
        entities=[Entity(text="Иван", label="PER", start=0, end=4, confidence=0.8, normalized="иван")],
        classification=_success_classification(),
    )
    repository.set_enrichment(
        "doc-b",
        entities=[],
        classification=ClassificationResult(ClassLabel.OTHER, 0.0, "it"),
        enrichment_status=ProcessingStatus.FAILED,
        enrichment_error_code="NER_RuntimeError",
    )
    repository.set_summary(
        "doc-b",
        SummaryResult(
            summary=None,
            status=ProcessingStatus.FAILED,
            error_code="GigaChatServerError",
            updated_at=now,
        ),
    )
    repository.set_hourly_digest_link(["doc-a", "doc-b"], "digest-1")
    repository.set_dedup_metadata_bulk(
        [
            DedupMetadataUpdate("doc-a", True, "doc-a", 1.0),
            DedupMetadataUpdate("doc-b", False, "doc-a", 0.93),
        ],
        updated_at=now,
    )
    opensearch_client.indices.refresh(index=index_name)

    doc_a = opensearch_client.get(index=index_name, id="doc-a")["_source"]
    doc_b = opensearch_client.get(index=index_name, id="doc-b")["_source"]
    assert doc_a["entities"][0]["label"] == "PER"
    assert doc_a["hourly_digest_id"] == "digest-1"
    assert doc_b["summary_status"] == "failed"
    assert doc_b["summary_error_code"] == "GigaChatServerError"
    assert doc_b["enrichment_status"] == "failed"
    assert doc_b["enrichment_error_code"] == "NER_RuntimeError"
    assert doc_b["dedup_is_canonical"] is False


def test_news_repository_set_dedup_metadata_bulk_is_best_effort(opensearch_client, indexed_os_names) -> None:
    index_name = indexed_os_names["news_index"]
    repository = NewsRepository(opensearch_client, index_name)
    now = datetime.now(timezone.utc)

    assert repository.upsert_news([_news_item("doc-ok", published_at=now)]) == 1

    result = repository.set_dedup_metadata_bulk(
        [
            DedupMetadataUpdate("doc-missing", False, "doc-root", 0.91),
            DedupMetadataUpdate("doc-ok", False, "doc-root", 0.82),
        ],
        updated_at=now,
    )

    assert result.attempted == 2
    assert result.updated == 1
    assert result.failed_ids == ["doc-missing"]

    opensearch_client.indices.refresh(index=index_name)
    payload = opensearch_client.get(index=index_name, id="doc-ok")["_source"]
    assert payload["dedup_is_canonical"] is False
    assert payload["dedup_canonical_external_id"] == "doc-root"
    assert payload["dedup_similarity_to_canonical"] == 0.82


def test_news_repository_query_methods_on_real_opensearch(opensearch_client, indexed_os_names) -> None:
    index_name = indexed_os_names["news_index"]
    repository = NewsRepository(opensearch_client, index_name)
    now = datetime.now(timezone.utc)

    items = [
        _news_item("recent-a", published_at=now - timedelta(minutes=50)),
        _news_item("recent-b", published_at=now - timedelta(hours=2, minutes=0)),
        _news_item("recent-c", published_at=now - timedelta(minutes=30)),
        _news_item("recent-d", published_at=now - timedelta(hours=3)),
        _news_item("old-e", published_at=now - timedelta(hours=30)),
    ]
    assert repository.upsert_news(items) == 5

    repository.set_summary(
        "recent-c",
        SummaryResult(
            summary="already done",
            status=ProcessingStatus.SUCCESS,
            error_code=None,
            updated_at=now,
        ),
    )
    repository.set_enrichment(
        "recent-c",
        entities=[Entity(text="Сбер", label="ORG", start=0, end=4, confidence=0.8, normalized="сбер")],
        classification=_success_classification(),
    )
    repository.set_enrichment(
        "recent-d",
        entities=[],
        classification=ClassificationResult(ClassLabel.OTHER, 0.0, "it"),
        enrichment_status=ProcessingStatus.FAILED,
        enrichment_error_code="CLASSIFICATION_RuntimeError",
    )
    repository.set_dedup_metadata_bulk(
        [
            DedupMetadataUpdate("recent-b", False, "recent-a", 0.92),
            DedupMetadataUpdate("recent-a", True, "recent-a", 1.0),
        ],
        updated_at=now,
    )
    opensearch_client.indices.refresh(index=index_name)

    no_summary = {row["external_id"] for row in repository.get_recent_news_without_summary(limit=50)}
    assert no_summary == {"recent-a", "recent-b", "recent-d"}

    no_summary_canonical = {row["external_id"] for row in repository.get_recent_canonical_news_without_summary(limit=50)}
    assert no_summary_canonical == {"recent-a", "recent-d"}

    no_enrichment = {row["external_id"] for row in repository.get_recent_news_without_enrichment(limit=50, hours=24)}
    assert no_enrichment == {"recent-a", "recent-b", "recent-d"}

    last_hour = {row["external_id"] for row in repository.get_news_for_last_hour(limit=50)}
    assert last_hour == {"recent-a", "recent-c"}

    last_hour_canonical = {row["external_id"] for row in repository.get_canonical_news_for_last_hour(limit=50)}
    assert last_hour_canonical == {"recent-a", "recent-c"}

    last_4h = [row["external_id"] for row in repository.get_news_for_last_hours(hours=4, limit=50)]
    assert set(last_4h) == {"recent-a", "recent-b", "recent-c", "recent-d"}

    last_4h_canonical = {row["external_id"] for row in repository.get_canonical_news_for_last_hours(hours=4, limit=50)}
    assert last_4h_canonical == {"recent-a", "recent-c", "recent-d"}

    dedup_candidates = [row["external_id"] for row in repository.get_news_for_dedup_candidates(lookback_hours=4, limit=50)]
    assert set(dedup_candidates) == {"recent-a", "recent-b", "recent-c", "recent-d"}
    assert dedup_candidates[0] == "recent-d"


def test_hourly_digest_and_processing_state_repositories_roundtrip(opensearch_client, indexed_os_names) -> None:
    digests_index = indexed_os_names["digests_index"]
    state_index = indexed_os_names["state_index"]

    digest_repository = HourlyDigestRepository(opensearch_client, digests_index)
    digest_repository.upsert(
        HourlyDigest(
            digest_id="digest-2026050110",
            window_start=datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc),
            window_end=datetime(2026, 5, 1, 10, 0, tzinfo=timezone.utc),
            summary="hour summary",
            news_ids=["a", "b"],
        )
    )
    opensearch_client.indices.refresh(index=digests_index)
    digest_doc = opensearch_client.get(index=digests_index, id="digest-2026050110")["_source"]
    assert digest_doc["summary"] == "hour summary"
    assert digest_doc["news_ids"] == ["a", "b"]

    state_repository = ProcessingStateRepository(opensearch_client, state_index)
    assert state_repository.get_state("cursor-1") is None
    state_repository.put_state("cursor-1", {"offset": 10, "status": "ok"})
    opensearch_client.indices.refresh(index=state_index)
    assert state_repository.get_state("cursor-1") == {"offset": 10, "status": "ok"}
