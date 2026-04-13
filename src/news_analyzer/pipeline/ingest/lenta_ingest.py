from __future__ import annotations

import logging

from news_analyzer.pipeline.ingest._status_policy import _finalize_ingest_status
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.sources.lenta.collector import LentaNewsCollector
from news_analyzer.sources.lenta.config import LentaCollectorConfig
from news_analyzer.sources.lenta.parser import parse_lenta_article
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.indices import OpenSearchIndexManager
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)


def run_lenta_ingest() -> int:
    logging.basicConfig(level=logging.INFO)
    settings = AppSettings.from_env()

    collector_config = LentaCollectorConfig.from_sources_file(settings.sources_config_path)
    collector = LentaNewsCollector(collector_config)

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
    OpenSearchIndexManager(client).ensure(settings.opensearch_news_index, "news.json")
    repository = NewsRepository(client, settings.opensearch_news_index)

    rows = collector.collect_latest()
    normalized = []
    for row in rows:
        try:
            normalized.append(parse_lenta_article(row))
        except Exception:  # noqa: BLE001
            logger.exception("Failed to parse Lenta row")

    created = repository.upsert_news(normalized)
    stats = collector.last_stats
    return _finalize_ingest_status(
        logger=logger,
        source_name="Lenta",
        created=created,
        collected_rows=len(rows),
        normalized_rows=len(normalized),
        fatal_errors=stats.fatal_errors,
        fatal_error_message="Lenta ingest had fatal fetch failures " f"(fetch_errors={stats.fetch_errors})",
        extra_quality_metrics={
            "fetch_errors": stats.fetch_errors,
            "fetched": stats.fetched,
            "parsed": stats.parsed,
            "full_text_ok": stats.full_text_ok,
            "skipped_no_full_text": stats.skipped_no_full_text,
        },
    )
