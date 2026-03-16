from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from opensearchpy import OpenSearch

from news_analyzer.apps.streamlit.view_models import HourlyDigestView, NewsCard, NewsCursor, NewsPage


class StreamlitQueryService:
    def __init__(self, client: OpenSearch, news_index: str, digest_index: str) -> None:
        self._client = client
        self._news_index = news_index
        self._digest_index = digest_index

    def latest_news_page(
        self,
        size: int = 50,
        cursor: NewsCursor | None = None,
        source: str | None = None,
        class_label: str | None = None,
    ) -> NewsPage:
        must: list[dict[str, object]] = []
        if source:
            must.append({"term": {"source_type": source}})
        if class_label:
            must.append({"term": {"class_label": class_label}})

        query = {"match_all": {}} if not must else {"bool": {"must": must}}
        body: dict[str, Any] = {
            "size": size + 1,
            "query": query,
            "sort": [{"published_at": {"order": "desc"}}, {"external_id": {"order": "asc"}}],
        }
        if cursor:
            body["search_after"] = cursor.to_search_after()

        response = self._client.search(index=self._news_index, body=body)
        hits = response["hits"]["hits"]
        has_more = len(hits) > size
        page_hits = hits[:size]
        items = [self._map_news_hit(hit) for hit in page_hits]

        next_cursor = None
        if has_more and page_hits:
            last_sort = page_hits[-1].get("sort")
            if isinstance(last_sort, (list, tuple)):
                next_cursor = NewsCursor.from_sort(last_sort)

        return NewsPage(items=items, next_cursor=next_cursor, has_more=has_more)

    def latest_hourly_digest_for_last_hour(self, now: datetime | None = None) -> HourlyDigestView | None:
        response = self._client.search(
            index=self._digest_index,
            body={"size": 1, "sort": [{"window_end": {"order": "desc"}}], "query": {"match_all": {}}},
        )
        hits = response["hits"]["hits"]
        if not hits:
            return None
        digest = self._map_digest_hit(hits[0])
        if digest is None:
            return None

        now_utc = now.astimezone(timezone.utc) if now else datetime.now(timezone.utc)
        if digest.window_end < now_utc - timedelta(hours=1):
            return None
        return digest

    def _map_news_hit(self, hit: dict[str, Any]) -> NewsCard:
        source = hit.get("_source", {})
        source_metadata = source.get("source_metadata", {})
        if not isinstance(source_metadata, dict):
            source_metadata = {}

        authors_raw = source_metadata.get("authors")
        authors = ""
        if isinstance(authors_raw, list):
            authors = ", ".join(str(value) for value in authors_raw if value)
        elif isinstance(authors_raw, str):
            authors = authors_raw

        title_raw = source_metadata.get("title")
        title = str(title_raw) if title_raw else str(source.get("external_id") or hit.get("_id"))
        url_raw = source_metadata.get("url") or source_metadata.get("permalink")

        return NewsCard(
            external_id=str(source.get("external_id") or hit.get("_id")),
            title=title,
            summary=source.get("summary") if isinstance(source.get("summary"), str) else None,
            class_label=source.get("class_label") if isinstance(source.get("class_label"), str) else None,
            published_at=self._parse_datetime(source.get("published_at")),
            source_type=source.get("source_type") if isinstance(source.get("source_type"), str) else None,
            raw_text=source.get("raw_text") if isinstance(source.get("raw_text"), str) else None,
            url=str(url_raw) if url_raw else None,
            authors=authors,
            section=source_metadata.get("section") if isinstance(source_metadata.get("section"), str) else None,
        )

    def _map_digest_hit(self, hit: dict[str, Any]) -> HourlyDigestView | None:
        source = hit.get("_source", {})
        window_start = self._parse_datetime(source.get("window_start"))
        window_end = self._parse_datetime(source.get("window_end"))
        if window_start is None or window_end is None:
            return None

        news_ids = source.get("news_ids")
        news_count = len(news_ids) if isinstance(news_ids, list) else 0
        summary = source.get("summary") if isinstance(source.get("summary"), str) else None
        return HourlyDigestView(
            digest_id=str(source.get("digest_id") or hit.get("_id")),
            window_start=window_start,
            window_end=window_end,
            summary=summary,
            news_count=news_count,
        )

    @staticmethod
    def _parse_datetime(value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
