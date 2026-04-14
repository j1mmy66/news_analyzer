from __future__ import annotations

import logging
import time

from news_analyzer.domain.enums import ClassLabel, ProcessingStatus
from news_analyzer.domain.models import ClassificationResult, Entity
from news_analyzer.nlp.classification.local_model import HFNewsClassificationModel
from news_analyzer.nlp.ner.local_model import NatashaSlovnetNERModel
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)
_TEMPLATE_PHRASE = "Самые важные новости"


def _trim_after_template_phrase(text: str) -> str:
    lower_text = text.lower()
    marker_index = lower_text.find(_TEMPLATE_PHRASE.lower())
    if marker_index == -1:
        return text
    return text[:marker_index].strip()


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
    classifier = HFNewsClassificationModel(
        model_path=settings.classifier_model_path,
        device=settings.classifier_device,
        max_length=settings.classifier_max_length,
    )

    processed = 0
    for item in repository.get_recent_news_without_enrichment(limit=limit, hours=24):
        external_id = str(item["external_id"])
        text = str(item.get("cleaned_text") or "")
        prepared_text = _trim_after_template_phrase(text)
        ner_error: Exception | None = None
        classification_error: Exception | None = None

        try:
            entities = _extract_with_retry(
                ner_model=ner_model,
                text=prepared_text,
                max_retries=settings.ner_max_retries,
                backoff_seconds=settings.ner_retry_backoff_seconds,
                backoff_cap_seconds=settings.ner_retry_backoff_cap_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            ner_error = exc
            logger.exception("NER failed for %s; storing empty entities", external_id)
            entities = []

        try:
            classification = classifier.classify(prepared_text)
        except Exception as exc:  # noqa: BLE001
            classification_error = exc
            logger.exception("Classification failed for %s; storing OTHER label", external_id)
            classification = ClassificationResult(
                class_label=ClassLabel.OTHER,
                class_confidence=0.0,
                model_version="hf-any-news-v1",
            )

        if ner_error and classification_error:
            enrichment_status = ProcessingStatus.FAILED
            enrichment_error_code: str | None = "NER_AND_CLASSIFICATION_FAILED"
        elif ner_error:
            enrichment_status = ProcessingStatus.FAILED
            enrichment_error_code = f"NER_{ner_error.__class__.__name__}"
        elif classification_error:
            enrichment_status = ProcessingStatus.FAILED
            enrichment_error_code = f"CLASSIFICATION_{classification_error.__class__.__name__}"
        else:
            enrichment_status = ProcessingStatus.SUCCESS
            enrichment_error_code = None

        try:
            repository.set_enrichment(
                external_id,
                entities,
                classification,
                enrichment_status=enrichment_status,
                enrichment_error_code=enrichment_error_code,
            )
            processed += 1
        except Exception:  # noqa: BLE001
            logger.exception("Persistence failed for %s", external_id)

    logger.info("Enrichment completed for %s items", processed)
    return processed
