from __future__ import annotations

import logging
import time

from news_analyzer.domain.models import Entity
from news_analyzer.nlp.classification.local_model import KeywordClassificationModel
from news_analyzer.nlp.ner.local_model import NatashaSlovnetNERModel
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)


def _extract_with_retry(
    ner_model: NatashaSlovnetNERModel,
    text: str,
    max_retries: int,
    backoff_seconds: float,
    backoff_cap_seconds: float,
) -> list[Entity]:
    for attempt in range(max_retries + 1):
        try:
            return ner_model.extract(text)
        except Exception:  # noqa: BLE001
            if attempt >= max_retries:
                raise
            delay = min(backoff_seconds * (2**attempt), backoff_cap_seconds)
            logger.warning(
                "NER extraction attempt %s/%s failed; retrying in %.2f seconds",
                attempt + 1,
                max_retries + 1,
                delay,
            )
            time.sleep(delay)

    return []


def run_ner_job(limit: int = 300) -> int:
    logging.basicConfig(level=logging.INFO)
    settings = AppSettings.from_env()

    client = build_client(
        OpenSearchConfig(
            hosts=settings.opensearch_hosts,
            news_index=settings.opensearch_news_index,
            digests_index=settings.opensearch_digests_index,
            username=settings.opensearch_username,
            password=settings.opensearch_password,
            use_ssl=settings.opensearch_use_ssl,
            verify_certs=settings.opensearch_verify_certs,
        )
    )
    repository = NewsRepository(client, settings.opensearch_news_index)

    ner_model = NatashaSlovnetNERModel(
        slovnet_model_path=settings.ner_slovnet_model_path,
        navec_path=settings.ner_navec_path,
    )
    classifier = KeywordClassificationModel()

    processed = 0
    for item in repository.get_news_for_last_hour(limit=limit):
        external_id = str(item["external_id"])
        text = str(item.get("cleaned_text") or "")

        try:
            entities = _extract_with_retry(
                ner_model=ner_model,
                text=text,
                max_retries=settings.ner_max_retries,
                backoff_seconds=settings.ner_retry_backoff_seconds,
                backoff_cap_seconds=settings.ner_retry_backoff_cap_seconds,
            )
        except Exception:  # noqa: BLE001
            logger.exception("NER failed for %s; storing empty entities", external_id)
            entities = []

        try:
            classification = classifier.classify(text)
            repository.set_enrichment(external_id, entities, classification)
            processed += 1
        except Exception:  # noqa: BLE001
            logger.exception("Classification/persistence failed for %s", external_id)

    logger.info("Enrichment completed for %s items", processed)
    return processed
