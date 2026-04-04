from __future__ import annotations

from datetime import datetime, timezone

from news_analyzer.apps.streamlit.query_service import StreamlitQueryService
from news_analyzer.apps.streamlit.view_models import NewsCursor


class _FakeClient:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = responses
        self.calls: list[dict[str, object]] = []

    def search(self, index: str, body: dict[str, object]) -> dict[str, object]:
        self.calls.append({"index": index, "body": body})
        return self._responses.pop(0)


def _news_hit(external_id: str, published_at: str) -> dict[str, object]:
    return {
        "_id": external_id,
        "_source": {
            "external_id": external_id,
            "published_at": published_at,
            "source_type": "rbc",
            "source_metadata": {
                "title": f"title-{external_id}",
                "url": f"https://example.com/{external_id}",
                "authors": ["a1", "a2"],
                "section": "economics",
            },
            "raw_text": f"raw-{external_id}",
            "summary": f"summary-{external_id}",
            "class_label": "economy",
        },
        "sort": [published_at, external_id],
    }


def test_latest_news_page_uses_stable_sort_and_cursor() -> None:
    client = _FakeClient(
        [
            {"hits": {"hits": [_news_hit("id-2", "2026-03-16T10:00:00+00:00"), _news_hit("id-1", "2026-03-16T09:59:00+00:00")]}}
        ]
    )
    service = StreamlitQueryService(client=client, news_index="news_items", digest_index="hourly_digests")

    page = service.latest_news_page(size=1, cursor=NewsCursor("2026-03-16T11:00:00+00:00", "id-3"))

    assert len(page.items) == 1
    assert page.has_more is True
    assert page.next_cursor == NewsCursor("2026-03-16T10:00:00+00:00", "id-2")
    call_body = client.calls[0]["body"]
    assert call_body["size"] == 2
    assert call_body["sort"] == [{"published_at": {"order": "desc"}}, {"external_id": {"order": "asc"}}]
    assert call_body["search_after"] == ["2026-03-16T11:00:00+00:00", "id-3"]


def test_latest_news_page_applies_source_and_class_filters() -> None:
    client = _FakeClient([{"hits": {"hits": []}}])
    service = StreamlitQueryService(client=client, news_index="news_items", digest_index="hourly_digests")

    service.latest_news_page(size=50, source="rbc", class_label="economy")

    query = client.calls[0]["body"]["query"]
    assert query == {
        "bool": {
            "must": [
                {"term": {"source_type": "rbc"}},
                {"term": {"class_label": "economy"}},
            ]
        }
    }


def test_latest_news_page_returns_next_cursor_and_has_more_false_on_last_batch() -> None:
    client = _FakeClient([{"hits": {"hits": [_news_hit("id-1", "2026-03-16T09:59:00+00:00")]}}])
    service = StreamlitQueryService(client=client, news_index="news_items", digest_index="hourly_digests")

    page = service.latest_news_page(size=5)

    assert len(page.items) == 1
    assert page.has_more is False
    assert page.next_cursor is None


def test_latest_hourly_digest_for_last_hour_validates_window() -> None:
    now = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    recent_client = _FakeClient(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "digest-1",
                            "_source": {
                                "digest_id": "digest-1",
                                "window_start": "2026-03-17T07:00:00+00:00",
                                "window_end": "2026-03-17T07:59:00+00:00",
                                "summary": "ok",
                                "news_ids": ["id-1", "id-2"],
                            },
                        }
                    ]
                }
            }
        ]
    )
    stale_client = _FakeClient(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "digest-2",
                            "_source": {
                                "digest_id": "digest-2",
                                "window_start": "2026-03-17T05:00:00+00:00",
                                "window_end": "2026-03-17T05:59:00+00:00",
                                "summary": "old",
                                "news_ids": ["id-9"],
                            },
                        }
                    ]
                }
            }
        ]
    )

    recent_service = StreamlitQueryService(client=recent_client, news_index="news_items", digest_index="hourly_digests")
    stale_service = StreamlitQueryService(client=stale_client, news_index="news_items", digest_index="hourly_digests")

    recent = recent_service.latest_hourly_digest_for_last_hour(now=now)
    stale = stale_service.latest_hourly_digest_for_last_hour(now=now)

    assert recent is not None
    assert recent.news_count == 2
    assert stale is None


def test_latest_hourly_digest_returns_none_when_no_hits() -> None:
    client = _FakeClient([{"hits": {"hits": []}}])
    service = StreamlitQueryService(client=client, news_index="news_items", digest_index="hourly_digests")

    assert service.latest_hourly_digest_for_last_hour() is None


def test_latest_hourly_digest_returns_none_for_invalid_digest_dates() -> None:
    client = _FakeClient(
        [
            {
                "hits": {
                    "hits": [
                        {
                            "_id": "digest-x",
                            "_source": {
                                "window_start": "invalid",
                                "window_end": "2026-03-17T07:59:00+00:00",
                            },
                        }
                    ]
                }
            }
        ]
    )
    service = StreamlitQueryService(client=client, news_index="news_items", digest_index="hourly_digests")

    assert service.latest_hourly_digest_for_last_hour() is None


def test_map_news_hit_handles_non_dict_metadata_and_string_authors() -> None:
    client = _FakeClient([])
    service = StreamlitQueryService(client=client, news_index="news_items", digest_index="hourly_digests")

    card_no_meta = service._map_news_hit(
        {
            "_id": "id-1",
            "_source": {
                "external_id": "id-1",
                "published_at": "2026-03-16T10:00:00+00:00",
                "source_metadata": "bad-shape",
            },
        }
    )
    card_with_authors_str = service._map_news_hit(
        {
            "_id": "id-2",
            "_source": {
                "external_id": "id-2",
                "published_at": "2026-03-16T10:00:00+00:00",
                "source_metadata": {
                    "authors": "single-author",
                    "permalink": "https://example.com/id-2",
                },
            },
        }
    )

    assert card_no_meta.authors == ""
    assert card_no_meta.title == "id-1"
    assert card_with_authors_str.authors == "single-author"
    assert card_with_authors_str.url == "https://example.com/id-2"


def test_parse_datetime_handles_non_str_and_invalid_iso() -> None:
    assert StreamlitQueryService._parse_datetime(123) is None
    assert StreamlitQueryService._parse_datetime("not-a-date") is None
    parsed = StreamlitQueryService._parse_datetime("2026-03-17T08:00:00")
    assert parsed is not None
    assert parsed.tzinfo == timezone.utc
