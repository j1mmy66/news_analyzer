from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from news_analyzer.domain.enums import ProcessingStatus
from news_analyzer.domain.models import HourlyDigest
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.indices import OpenSearchIndexManager
from news_analyzer.storage.opensearch.repositories import HourlyDigestRepository, NewsRepository
from news_analyzer.summarization.gigachat.client import GigaChatClient
from news_analyzer.summarization.service import SummaryService

logger = logging.getLogger(__name__)


def run_hourly_digest_job() -> str | None:
    logging.basicConfig(level=logging.INFO)
    settings = AppSettings.from_env()
    auth_key = settings.gigachat_auth_key
    if not auth_key and settings.gigachat_api_key:
        logger.warning("GIGACHAT_API_KEY is deprecated, use GIGACHAT_AUTH_KEY")
        auth_key = settings.gigachat_api_key
    if not auth_key:
        logger.warning("GigaChat credentials are not configured; skipping hourly digest")
        return None

    os_config = OpenSearchConfig(
        hosts=settings.opensearch_hosts,
        news_index=settings.opensearch_news_index,
        digests_index=settings.opensearch_digests_index,
        username=settings.opensearch_username,
        password=settings.opensearch_password,
        use_ssl=settings.opensearch_use_ssl,
        verify_certs=settings.opensearch_verify_certs,
    )
    client = build_client(os_config)

    index_manager = OpenSearchIndexManager(client)
    index_manager.ensure(settings.opensearch_news_index, "news.json")
    index_manager.ensure(settings.opensearch_digests_index, "hourly_digests.json")

    news_repository = NewsRepository(client, settings.opensearch_news_index)
    digest_repository = HourlyDigestRepository(client, settings.opensearch_digests_index)

    items = news_repository.get_news_for_last_hour()
    texts = [str(item.get("cleaned_text") or "") for item in items if item.get("cleaned_text")]
    ids = [str(item["external_id"]) for item in items]
    if not texts:
        return None

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
    result = summary_service.summarize_hour(texts)
    if result.status != ProcessingStatus.SUCCESS or not result.summary:
        logger.warning("Hourly digest summary failed")
        return None

    now = datetime.now(timezone.utc)
    digest = HourlyDigest(
        digest_id=now.strftime("digest-%Y%m%d%H"),
        window_start=now - timedelta(hours=1),
        window_end=now,
        summary=result.summary,
        news_ids=ids,
    )
    digest_repository.upsert(digest)
    news_repository.set_hourly_digest_link(ids, digest.digest_id)
    return digest.digest_id
