from __future__ import annotations

import logging

from news_analyzer.nlp.classification.local_model import KeywordClassificationModel
from news_analyzer.nlp.ner.local_model import RegexNERModel
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)


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

    ner_model = RegexNERModel()
    classifier = KeywordClassificationModel()

    processed = 0
    for item in repository.get_news_for_last_hour(limit=limit):
        external_id = str(item["external_id"])
        text = str(item.get("cleaned_text") or "")
        try:
            entities = ner_model.extract(text)
            classification = classifier.classify(text)
            repository.set_enrichment(external_id, entities, classification)
            processed += 1
        except Exception:  # noqa: BLE001
            logger.exception("NER/classification failed for %s", external_id)

    logger.info("Enrichment completed for %s items", processed)
    return processed
