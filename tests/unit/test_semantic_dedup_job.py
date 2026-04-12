from __future__ import annotations

from datetime import datetime, timezone

from news_analyzer.domain.models import DedupMetadataUpdate
from news_analyzer.pipeline.dedup import semantic_dedup_job


class _Settings:
    opensearch_hosts = ["http://opensearch:9200"]
    opensearch_news_index = "news_items"
    opensearch_digests_index = "hourly_digests"
    opensearch_username = None
    opensearch_password = None
    opensearch_use_ssl = False
    opensearch_verify_certs = False
    dedup_model_name = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    dedup_similarity_threshold = 0.90
    dedup_window_hours = 3
    dedup_lookback_hours = 24
    dedup_text_chars = 1000
    dedup_device = "cpu"


def test_run_semantic_dedup_job_skips_when_no_candidates(monkeypatch) -> None:
    class _FakeIndexManager:
        def __init__(self, client: object) -> None:
            return None

        def ensure(self, index_name: str, mapping_file: str) -> None:
            assert index_name == "news_items"
            assert mapping_file == "news.json"

    class _FakeRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def get_news_for_dedup_candidates(self, *, lookback_hours: int, limit: int):
            assert lookback_hours == 24
            assert limit == 10
            return []

    monkeypatch.setattr(semantic_dedup_job.AppSettings, "from_env", classmethod(lambda cls: _Settings()))
    monkeypatch.setattr(semantic_dedup_job, "build_client", lambda config: object())
    monkeypatch.setattr(semantic_dedup_job, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(semantic_dedup_job, "NewsRepository", _FakeRepository)

    assert semantic_dedup_job.run_semantic_dedup_job(limit=10) == 0


def test_run_semantic_dedup_job_updates_repository(monkeypatch) -> None:
    captured: dict[str, object] = {"updates": None}

    class _FakeIndexManager:
        def __init__(self, client: object) -> None:
            return None

        def ensure(self, index_name: str, mapping_file: str) -> None:
            assert index_name == "news_items"
            assert mapping_file == "news.json"

    class _FakeRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def get_news_for_dedup_candidates(self, *, lookback_hours: int, limit: int):
            assert lookback_hours == 24
            assert limit == 50
            return [{"external_id": "id-1"}, {"external_id": "id-2"}]

        def set_dedup_metadata_bulk(self, updates, *, updated_at):
            captured["updates"] = list(updates)
            assert isinstance(updated_at, datetime)
            assert updated_at.tzinfo == timezone.utc

    class _FakeEmbeddingModel:
        def __init__(self, model_name: str, device: str) -> None:
            assert model_name == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
            assert device == "cpu"

    class _FakeDeduplicator:
        def __init__(self, embedding_model, *, similarity_threshold: float, window_hours: int, text_chars: int) -> None:
            assert similarity_threshold == 0.90
            assert window_hours == 3
            assert text_chars == 1000

        def deduplicate(self, items: list[dict[str, object]]):
            assert len(items) == 2
            return [
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
                    similarity_to_canonical=0.92,
                ),
            ]

    monkeypatch.setattr(semantic_dedup_job.AppSettings, "from_env", classmethod(lambda cls: _Settings()))
    monkeypatch.setattr(semantic_dedup_job, "build_client", lambda config: object())
    monkeypatch.setattr(semantic_dedup_job, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(semantic_dedup_job, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(semantic_dedup_job, "TransformerTextEmbeddingModel", _FakeEmbeddingModel)
    monkeypatch.setattr(semantic_dedup_job, "SemanticNewsDeduplicator", _FakeDeduplicator)

    result = semantic_dedup_job.run_semantic_dedup_job(limit=50)

    assert result == 2
    assert captured["updates"] is not None
    assert len(captured["updates"]) == 2
