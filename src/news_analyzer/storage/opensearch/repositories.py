from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from typing import Iterable

from opensearchpy import OpenSearch

from news_analyzer.domain.models import ClassificationResult, Entity, HourlyDigest, NormalizedNewsItem, SummaryResult

logger = logging.getLogger(__name__)


class NewsRepository:
    def __init__(self, client: OpenSearch, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    def upsert_news(self, items: Iterable[NormalizedNewsItem]) -> int:
        success = 0
        for item in items:
            body = {
                "source_type": item.source_type.value,
                "external_id": item.external_id,
                "published_at": item.published_at.astimezone(timezone.utc).isoformat(),
                "source_metadata": item.source_metadata,
                "raw_text": item.raw_text,
                "cleaned_text": item.cleaned_text,
            }
            self._client.index(index=self._index_name, id=item.external_id, body=body)
            success += 1
        return success

    def set_enrichment(self, external_id: str, entities: list[Entity], classification: ClassificationResult) -> None:
        body = {
            "doc": {
                "entities": [asdict(value) for value in entities],
                "class_label": classification.class_label.value,
                "class_confidence": classification.class_confidence,
            }
        }
        self._client.update(index=self._index_name, id=external_id, body=body)

    def set_summary(self, external_id: str, summary: SummaryResult) -> None:
        body = {
            "doc": {
                "summary": summary.summary,
                "summary_status": summary.status.value,
                "summary_error_code": summary.error_code,
                "summary_updated_at": summary.updated_at.astimezone(timezone.utc).isoformat(),
            }
        }
        self._client.update(index=self._index_name, id=external_id, body=body)

    def set_hourly_digest_link(self, external_ids: list[str], digest_id: str) -> None:
        for external_id in external_ids:
            self._client.update(
                index=self._index_name,
                id=external_id,
                body={"doc": {"hourly_digest_id": digest_id}},
            )

    def get_recent_news_without_summary(self, limit: int = 100) -> list[dict[str, object]]:
        response = self._client.search(
            index=self._index_name,
            body={
                "size": limit,
                "query": {
                    "bool": {
                        "must": [
                            {
                                "range": {
                                    "published_at": {
                                        "gte": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
                                    }
                                }
                            }
                        ],
                        "must_not": [{"exists": {"field": "summary"}}],
                    }
                },
                "sort": [{"published_at": {"order": "desc"}}],
            },
        )
        return [hit["_source"] | {"external_id": hit["_id"]} for hit in response["hits"]["hits"]]

    def get_news_for_last_hour(self, now: datetime | None = None, limit: int = 300) -> list[dict[str, object]]:
        now_utc = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        one_hour_ago = now_utc - timedelta(hours=1)

        response = self._client.search(
            index=self._index_name,
            body={
                "size": limit,
                "query": {
                    "range": {
                        "published_at": {
                            "gte": one_hour_ago.isoformat(),
                            "lte": now_utc.isoformat(),
                        }
                    }
                },
                "sort": [{"published_at": {"order": "asc"}}],
            },
        )
        return [hit["_source"] | {"external_id": hit["_id"]} for hit in response["hits"]["hits"]]


class HourlyDigestRepository:
    def __init__(self, client: OpenSearch, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    def upsert(self, digest: HourlyDigest) -> None:
        self._client.index(
            index=self._index_name,
            id=digest.digest_id,
            body={
                "digest_id": digest.digest_id,
                "window_start": digest.window_start.astimezone(timezone.utc).isoformat(),
                "window_end": digest.window_end.astimezone(timezone.utc).isoformat(),
                "summary": digest.summary,
                "news_ids": digest.news_ids,
            },
        )


class ProcessingStateRepository:
    """Reserved for processing offsets/checkpoints in future iterations."""

    def __init__(self, client: OpenSearch, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    def put_state(self, state_id: str, payload: dict[str, object]) -> None:
        self._client.index(index=self._index_name, id=state_id, body=payload)

    def get_state(self, state_id: str) -> dict[str, object] | None:
        if not self._client.exists(index=self._index_name, id=state_id):
            return None
        return self._client.get(index=self._index_name, id=state_id).get("_source")
