from __future__ import annotations

import logging
from datetime import datetime, timezone

from news_analyzer.nlp.dedup.semantic import SemanticNewsDeduplicator, TransformerTextEmbeddingModel
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.indices import OpenSearchIndexManager
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)


def run_semantic_dedup_job(limit: int = 5000) -> int:
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
    OpenSearchIndexManager(client).ensure(settings.opensearch_news_index, "news.json")
    repository = NewsRepository(client, settings.opensearch_news_index)
    items = repository.get_news_for_dedup_candidates(lookback_hours=settings.dedup_lookback_hours, limit=limit)
    if not items:
        logger.info("Semantic dedup skipped: no candidates found")
        return 0

    deduplicator = SemanticNewsDeduplicator(
        embedding_model=TransformerTextEmbeddingModel(
            model_name=settings.dedup_model_name,
            device=settings.dedup_device,
        ),
        similarity_threshold=settings.dedup_similarity_threshold,
        window_hours=settings.dedup_window_hours,
        text_chars=settings.dedup_text_chars,
    )
    try:
        updates = deduplicator.deduplicate(items)
    except Exception:  # noqa: BLE001
        logger.exception("Semantic dedup failed: unable to compute updates for batch_size=%s", len(items))
        return 0
    if not updates:
        logger.info("Semantic dedup skipped: no valid items after preprocessing")
        return 0

    result = repository.set_dedup_metadata_bulk(updates, updated_at=datetime.now(timezone.utc))
    if result.failed_ids:
        logger.warning(
            "Semantic dedup completed with partial failures: attempted=%s updated=%s failed=%s failed_ids=%s",
            result.attempted,
            result.updated,
            len(result.failed_ids),
            ",".join(result.failed_ids),
        )
    else:
        logger.info("Semantic dedup processed %s items", result.updated)
    return result.updated
