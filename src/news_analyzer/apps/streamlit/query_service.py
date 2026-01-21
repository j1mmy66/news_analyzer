from __future__ import annotations

from opensearchpy import OpenSearch


class StreamlitQueryService:
    def __init__(self, client: OpenSearch, news_index: str, digest_index: str) -> None:
        self._client = client
        self._news_index = news_index
        self._digest_index = digest_index

    def latest_news(self, size: int = 50, source: str | None = None, class_label: str | None = None) -> list[dict[str, object]]:
        must: list[dict[str, object]] = []
        if source:
            must.append({"term": {"source_type": source}})
        if class_label:
            must.append({"term": {"class_label": class_label}})

        query = {"match_all": {}} if not must else {"bool": {"must": must}}

        response = self._client.search(
            index=self._news_index,
            body={"size": size, "query": query, "sort": [{"published_at": {"order": "desc"}}]},
        )
        return [hit["_source"] | {"external_id": hit["_id"]} for hit in response["hits"]["hits"]]

    def latest_hourly_digest(self) -> dict[str, object] | None:
        response = self._client.search(
            index=self._digest_index,
            body={"size": 1, "sort": [{"window_end": {"order": "desc"}}], "query": {"match_all": {}}},
        )
        hits = response["hits"]["hits"]
        if not hits:
            return None
        hit = hits[0]
        return hit["_source"] | {"digest_id": hit["_id"]}
