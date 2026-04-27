from __future__ import annotations

from datetime import UTC, datetime
import xml.etree.ElementTree as ET

import pytest

from news_analyzer.sources.lenta.collector import LentaFetchError, LentaNewsCollector, _build_retry
from news_analyzer.sources.lenta.config import LentaCollectorConfig


def _rss_item(*, title: str = "Title", link: str = "https://lenta.ru/news/2026/04/26/1", pub_date: str = "Sun, 26 Apr 2026 19:49:08 +0300", description: str = "Short", categories: list[str] | None = None) -> ET.Element:
    cats = "".join(f"<category>{value}</category>" for value in (categories or []))
    xml = f"""
<item>
  <title>{title}</title>
  <link>{link}</link>
  <pubDate>{pub_date}</pubDate>
  <description>{description}</description>
  {cats}
</item>
"""
    return ET.fromstring(xml)


def test_to_record_from_rss_item_maps_fields() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())

    record = collector._to_record_from_rss_item(_rss_item(categories=["Россия", "Политика"]))

    assert record is not None
    assert record["url"] == "https://lenta.ru/news/2026/04/26/1"
    assert record["title"] == "Title"
    assert record["body"] == "Short"
    assert record["section"] == "Россия"
    assert isinstance(record["published_at"], datetime)


def test_to_record_from_rss_item_handles_missing_optional_categories() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())

    record = collector._to_record_from_rss_item(_rss_item(categories=[]))

    assert record is not None
    assert record["section"] == "lenta"


def test_to_record_from_rss_item_returns_none_for_invalid_payload() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())

    assert collector._to_record_from_rss_item(_rss_item(title="")) is None
    assert collector._to_record_from_rss_item(_rss_item(link="")) is None
    assert collector._to_record_from_rss_item(_rss_item(pub_date="bad-date")) is None


def test_collect_latest_deduplicates_urls_and_skips_missing_full_text() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig(items_limit=10))
    collector._fetch_rss_items = lambda: [  # type: ignore[method-assign]
        _rss_item(link="https://lenta.ru/news/2026/04/26/1"),
        _rss_item(link="https://lenta.ru/news/2026/04/26/1", title="Dup"),
        _rss_item(link="https://lenta.ru/news/2026/04/26/2", title="No text"),
    ]

    full_text_map = {
        "https://lenta.ru/news/2026/04/26/1": "Full text #1",
        "https://lenta.ru/news/2026/04/26/2": None,
    }
    collector._fetch_full_text = lambda url: (full_text_map[url], "ok" if full_text_map[url] else "empty_text")  # type: ignore[method-assign]

    records = collector.collect_latest()

    assert len(records) == 1
    assert records[0]["url"] == "https://lenta.ru/news/2026/04/26/1"
    assert records[0]["body"] == "Full text #1"
    assert collector.last_stats.fetched == 3
    assert collector.last_stats.parsed == 3
    assert collector.last_stats.full_text_ok == 1
    assert collector.last_stats.skipped_no_full_text == 1
    assert collector.last_stats.skipped_empty_text == 1


def test_collect_latest_marks_fatal_on_rss_fetch_error() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())
    collector._fetch_rss_items = lambda: (_ for _ in ()).throw(LentaFetchError("rss timeout"))  # type: ignore[method-assign]

    records = collector.collect_latest()

    assert records == []
    assert collector.last_stats.fatal_errors == 1
    assert collector.last_stats.fetch_errors == 1


def test_fetch_rss_items_wraps_exceptions() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())
    collector._session.get = lambda url, timeout: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]

    with pytest.raises(LentaFetchError, match="boom"):
        collector._fetch_rss_items()


def test_fetch_full_text_increments_fetch_errors_on_exception() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())
    collector._session.get = lambda url, timeout: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]

    assert collector._fetch_full_text("https://lenta.ru/news/1") == (None, "fetch_error")
    assert collector.last_stats.fetch_errors == 1


def test_extract_full_text_handles_challenge_and_selectors() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())

    assert collector._extract_full_text("<html>captcha <div>challenge</div></html>") == (None, "challenge_block")

    html = "<html><body><div itemprop='articleBody'><p>Line 1</p><p>Line 2</p></div></body></html>"
    assert collector._extract_full_text(html) == ("Line 1 Line 2", "ok")

    fallback_html = "<html><body><p>A</p><p>B</p></body></html>"
    assert collector._extract_full_text(fallback_html) == ("A B", "ok")


def test_build_retry_uses_non_negative_values() -> None:
    retry = _build_retry(LentaCollectorConfig(max_retries=-1, backoff_seconds=-2.0))
    assert retry.total == 0
    assert retry.backoff_factor == 0.0


def test_fetch_rss_items_returns_empty_when_channel_missing() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig())

    class _Response:
        text = "<rss version='2.0'></rss>"

        def raise_for_status(self) -> None:
            return None

    collector._session.get = lambda url, timeout: _Response()  # type: ignore[method-assign]

    assert collector._fetch_rss_items() == []


def test_collect_latest_respects_items_limit() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig(items_limit=1))
    collector._fetch_rss_items = lambda: [  # type: ignore[method-assign]
        _rss_item(link="https://lenta.ru/news/2026/04/26/1"),
        _rss_item(link="https://lenta.ru/news/2026/04/26/2"),
    ]
    collector._fetch_full_text = lambda url: ("full-text", "ok")  # type: ignore[method-assign]

    rows = collector.collect_latest()

    assert len(rows) == 1
    assert rows[0]["published_at"].tzinfo == UTC


def test_collect_latest_tracks_skip_reasons() -> None:
    collector = LentaNewsCollector(LentaCollectorConfig(items_limit=10))
    collector._fetch_rss_items = lambda: [  # type: ignore[method-assign]
        _rss_item(link="https://lenta.ru/news/2026/04/26/1"),
        _rss_item(link="https://lenta.ru/news/2026/04/26/2"),
        _rss_item(link="https://lenta.ru/news/2026/04/26/3"),
    ]

    responses = {
        "https://lenta.ru/news/2026/04/26/1": (None, "fetch_error"),
        "https://lenta.ru/news/2026/04/26/2": (None, "challenge_block"),
        "https://lenta.ru/news/2026/04/26/3": (None, "empty_text"),
    }
    collector._fetch_full_text = lambda url: responses[url]  # type: ignore[method-assign]

    rows = collector.collect_latest()

    assert rows == []
    assert collector.last_stats.skipped_no_full_text == 3
    assert collector.last_stats.skipped_fetch_error == 1
    assert collector.last_stats.skipped_challenge == 1
    assert collector.last_stats.skipped_empty_text == 1
