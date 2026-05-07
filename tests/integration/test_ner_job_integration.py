from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
from pathlib import Path

import pytest

from news_analyzer.domain.enums import SourceType
from news_analyzer.domain.models import NormalizedNewsItem
from news_analyzer.pipeline.enrich import ner_job
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.repositories import NewsRepository


def _ensure_ner_runtime_available() -> None:
    required_modules = ("natasha", "navec", "slovnet", "transformers", "torch")
    for module_name in required_modules:
        if importlib.util.find_spec(module_name) is None:
            pytest.skip(f"Missing runtime dependency: {module_name}")

    required_paths = (
        Path("models/slovnet_ner_news_v1.tar"),
        Path("models/navec_news_v1_1B_250K_300d_100q.tar"),
        Path("models/any-news-classifier"),
    )
    for model_path in required_paths:
        if not model_path.exists():
            pytest.skip(f"Missing model artifact: {model_path}")


def _sample_item(external_id: str, published_at: datetime, cleaned_text: str) -> NormalizedNewsItem:
    return NormalizedNewsItem(
        source_type=SourceType.RBC,
        external_id=external_id,
        published_at=published_at,
        source_metadata={"url": f"https://example.com/{external_id}"},
        raw_text=cleaned_text,
        cleaned_text=cleaned_text,
    )


def test_run_ner_job_with_real_models_short_dataset(monkeypatch, opensearch_client, indexed_os_names) -> None:
    _ensure_ner_runtime_available()

    news_index = indexed_os_names["news_index"]
    repository = NewsRepository(opensearch_client, news_index)
    now = datetime.now(timezone.utc)

    items = [
        _sample_item(
            "ner-1",
            now - timedelta(minutes=25),
            "Президент Владимир Путин встретился в Москве с делегацией Сбера.",
        ),
        _sample_item(
            "ner-2",
            now - timedelta(minutes=20),
            "Компания Яндекс открыла новый офис в Санкт-Петербурге.",
        ),
        _sample_item(
            "ner-3",
            now - timedelta(minutes=15),
            "Министр Сергей Лавров выступил в ООН.",
        ),
    ]
    assert repository.upsert_news(items) == 3
    opensearch_client.indices.refresh(index=news_index)

    settings = AppSettings(
        opensearch_hosts=["http://localhost:19200"],
        opensearch_news_index=news_index,
        opensearch_digests_index=indexed_os_names["digests_index"],
        opensearch_username=None,
        opensearch_password=None,
        opensearch_use_ssl=False,
        opensearch_verify_certs=False,
        classifier_model_path=Path("models/any-news-classifier"),
        classifier_device="cpu",
        classifier_max_length=256,
        ner_slovnet_model_path=Path("models/slovnet_ner_news_v1.tar"),
        ner_navec_path=Path("models/navec_news_v1_1B_250K_300d_100q.tar"),
        ner_max_retries=1,
        ner_retry_backoff_seconds=0.1,
        ner_retry_backoff_cap_seconds=0.2,
    )
    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))

    processed = ner_job.run_ner_job(limit=10)
    assert processed == 3
    opensearch_client.indices.refresh(index=news_index)

    docs = [opensearch_client.get(index=news_index, id=item.external_id)["_source"] for item in items]
    labels_seen: set[str] = set()
    non_empty_entities_docs = 0

    for doc in docs:
        assert isinstance(doc.get("class_label"), str)
        assert isinstance(doc.get("class_confidence"), (int, float))
        assert doc.get("enrichment_status") in {"success", "failed"}

        entities = doc.get("entities")
        assert isinstance(entities, list)
        if entities:
            non_empty_entities_docs += 1
        for entity in entities:
            assert isinstance(entity.get("text"), str)
            assert isinstance(entity.get("label"), str)
            assert isinstance(entity.get("start"), int)
            assert isinstance(entity.get("end"), int)
            labels_seen.add(entity["label"])

    assert non_empty_entities_docs >= 1
    assert labels_seen.intersection({"PER", "ORG", "LOC"})
