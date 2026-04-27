from __future__ import annotations

import logging

from news_analyzer.pipeline.ingest._status_policy import _finalize_ingest_status
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.sources.rbc.collector import RBCNewsCollector
from news_analyzer.sources.rbc.config import RBCCollectorConfig
from news_analyzer.sources.rbc.parser import parse_rbc_article
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.indices import OpenSearchIndexManager
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)


def run_rbc_ingest() -> int:
    logging.basicConfig(level=logging.INFO)
    settings = AppSettings.from_env()

    collector_config = RBCCollectorConfig.from_sources_file(settings.sources_config_path)
    collector = RBCNewsCollector(collector_config)

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
            normalized.append(parse_rbc_article(row))
        except Exception:  # noqa: BLE001
            logger.exception("Failed to parse RBC row")

    created = repository.upsert_news(normalized)
    stats = collector.last_stats
    return _finalize_ingest_status(
        logger=logger,
        source_name="RBC",
        created=created,
        collected_rows=len(rows),
        normalized_rows=len(normalized),
        fatal_errors=stats.fatal_errors,
        fatal_error_message=(
            "RBC ingest had fatal fetch failures "
            f"(sections={stats.failed_sections}, fetch_errors={stats.fetch_errors_total})"
        ),
        extra_quality_metrics={
            "fetch_errors_total": stats.fetch_errors_total,
            "failed_sections": stats.failed_sections,
        },
    )
