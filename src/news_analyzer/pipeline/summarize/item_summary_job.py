from __future__ import annotations

import logging

from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.repositories import NewsRepository
from news_analyzer.summarization.gigachat.client import GigaChatClient
from news_analyzer.summarization.service import SummaryService

logger = logging.getLogger(__name__)


def run_item_summary_job(limit: int = 100) -> int:
    logging.basicConfig(level=logging.INFO)
    settings = AppSettings.from_env()
    auth_key = settings.gigachat_auth_key
    if not auth_key and settings.gigachat_api_key:
        logger.warning("GIGACHAT_API_KEY is deprecated, use GIGACHAT_AUTH_KEY")
        auth_key = settings.gigachat_api_key
    if not auth_key:
        logger.warning("GigaChat credentials are not configured; skipping summaries")
        return 0

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

    summary_service = SummaryService(
        GigaChatClient(
            auth_key=auth_key,
            scope=settings.gigachat_scope,
            model=settings.gigachat_model,
            timeout_seconds=settings.gigachat_timeout_seconds,
            max_retries=settings.gigachat_max_retries,
            verify_ssl=settings.gigachat_verify_ssl,
        )
    )

    processed = 0
    for item in repository.get_recent_news_without_summary(limit=limit):
        external_id = str(item["external_id"])
        text = str(item.get("cleaned_text") or "")
        result = summary_service.summarize_item(text)
        repository.set_summary(external_id, result)
        processed += 1

    logger.info("Item summaries processed: %s", processed)
    return processed
