from datetime import UTC, datetime

import pytest

from news_analyzer.sources.rbc.collector import RBCFetchError, RBCNewsCollector, _build_retry
from news_analyzer.sources.rbc.config import RBCCollectorConfig


def test_to_record_maps_search_item_to_rbc_contract() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._get_article_data = lambda _: ("Short overview", "Full article text")  # type: ignore[method-assign]

    record = collector._to_record_from_search_item(
        {
            "fronturl": "https://www.rbc.ru/economics/2026/03/15/example",
            "title": "RBC title",
            "publish_date_t": "2026-03-15T10:00:00+03:00",
            "authors": ["Author"],
        },
        section="economics",
    )

    assert record is not None
    assert record["url"] == "https://www.rbc.ru/economics/2026/03/15/example"
    assert record["title"] == "RBC title"
    assert record["body"] == "Full article text"
    assert record["section"] == "economics"
    assert record["authors"] == ["Author"]


def test_parse_published_at_accepts_milliseconds_timestamp() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))

    timestamp_ms = 1_773_571_200_000
    published_at = collector._parse_published_at({"publish_date_t": timestamp_ms})
    assert published_at == datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)


def test_collect_latest_uses_fallback_when_primary_fails() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._fetch_search_items = lambda **_: (_ for _ in ()).throw(RBCFetchError("ssl eof"))  # type: ignore[method-assign]
    collector._fetch_fallback_section_records = lambda **_: [  # type: ignore[method-assign]
        {
            "url": "https://www.rbc.ru/economics/2026/03/15/fallback",
            "title": "Fallback title",
            "body": "Fallback body",
            "published_at": datetime(2026, 3, 15, 0, 0, tzinfo=UTC),
            "authors": [],
            "section": "economics",
        }
    ]

    records = collector.collect_latest()

    assert len(records) == 1
    assert collector.last_stats.fallback_records == 1
    assert collector.last_stats.fatal_errors == 0
    assert collector.last_stats.fetch_errors_total == 1


def test_collect_latest_marks_section_fatal_when_primary_and_fallback_fail() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._fetch_search_items = lambda **_: (_ for _ in ()).throw(RBCFetchError("ssl eof"))  # type: ignore[method-assign]
    collector._fetch_fallback_section_records = lambda **_: []  # type: ignore[method-assign]

    records = collector.collect_latest()

    assert records == []
    assert collector.last_stats.fatal_errors == 1
    assert collector.last_stats.failed_sections == ["economics"]


def test_collect_section_stops_when_no_items() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=3))
    calls = {"value": 0}

    def _fetch(**kwargs):
        calls["value"] += 1
        if calls["value"] == 1:
            return [{"fronturl": "https://www.rbc.ru/1", "title": "T1", "publish_date_t": "2026-03-15T10:00:00+00:00"}]
        return []

    collector._fetch_search_items = _fetch  # type: ignore[method-assign]
    collector._get_article_data = lambda url: (None, "body")  # type: ignore[method-assign]

    rows, failed = collector._collect_section("economics", "14.03.2026", "15.03.2026")

    assert failed is False
    assert len(rows) == 1
    assert collector.last_stats.primary_records == 1


def test_collect_section_deduplicates_urls() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._fetch_search_items = lambda **kwargs: [  # type: ignore[method-assign]
        {"fronturl": "https://www.rbc.ru/dup", "title": "T1", "publish_date_t": "2026-03-15T10:00:00+00:00"},
        {"fronturl": "https://www.rbc.ru/dup", "title": "T2", "publish_date_t": "2026-03-15T10:00:00+00:00"},
    ]
    collector._get_article_data = lambda url: (None, "body")  # type: ignore[method-assign]

    rows, failed = collector._collect_section("economics", "14.03.2026", "15.03.2026")

    assert failed is False
    assert len(rows) == 1


def test_collect_section_marks_fatal_when_fallback_disabled() -> None:
    collector = RBCNewsCollector(
        RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1, fallback_enabled=False)
    )
    collector._fetch_search_items = lambda **kwargs: (_ for _ in ()).throw(RBCFetchError("network"))  # type: ignore[method-assign]

    rows, failed = collector._collect_section("economics", "14.03.2026", "15.03.2026")

    assert rows == []
    assert failed is True


def test_fetch_search_items_filters_non_dict_items() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {"items": [{"title": "ok"}, "bad", 123]}

    collector._session.get = lambda url, timeout: _Response()  # type: ignore[method-assign]

    items = collector._fetch_search_items("economics", "14.03.2026", "15.03.2026", page=0)
    assert items == [{"title": "ok"}]


def test_fetch_search_items_returns_empty_when_items_is_not_list() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {"items": {"unexpected": "shape"}}

    collector._session.get = lambda url, timeout: _Response()  # type: ignore[method-assign]
    assert collector._fetch_search_items("economics", "14.03.2026", "15.03.2026", page=0) == []


def test_fetch_search_items_wraps_exceptions() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._session.get = lambda url, timeout: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]

    with pytest.raises(RBCFetchError, match="boom"):
        collector._fetch_search_items("economics", "14.03.2026", "15.03.2026", page=0)


def test_fetch_fallback_section_records_parses_jsonld_and_filters_by_day() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    html = """
    <html><body>
      <script type="application/ld+json">{"@type":"NewsArticle","url":"https://www.rbc.ru/a","headline":"A","articleBody":"Body A","datePublished":"2026-03-15T10:00:00+00:00","author":{"name":"Auth1"}}</script>
      <script type="application/ld+json">[{"@type":"NewsArticle","url":"https://www.rbc.ru/a","headline":"A duplicate","datePublished":"2026-03-15T11:00:00+00:00"}]</script>
      <script type="application/ld+json">{"@type":"NewsArticle","url":"https://www.rbc.ru/old","headline":"Old","datePublished":"2026-03-10T10:00:00+00:00"}</script>
      <script type="application/ld+json">{not-json}</script>
    </body></html>
    """

    class _Response:
        text = html

        def raise_for_status(self) -> None:
            return None

    collector._session.get = lambda url, timeout: _Response()  # type: ignore[method-assign]

    rows = collector._fetch_fallback_section_records("economics", "14.03.2026", "15.03.2026")
    assert len(rows) == 1
    assert rows[0]["url"] == "https://www.rbc.ru/a"
    assert rows[0]["authors"] == ["Auth1"]


def test_to_record_from_search_item_returns_none_without_required_fields() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    assert collector._to_record_from_search_item({"title": "no url"}, section="economics") is None
    assert collector._to_record_from_search_item({"fronturl": "https://www.rbc.ru/1"}, section="economics") is None


def test_to_record_from_search_item_uses_announce_or_title_as_body() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._get_article_data = lambda url: ("", "")  # type: ignore[method-assign]

    record = collector._to_record_from_search_item(
        {
            "fronturl": "https://www.rbc.ru/economics/2026/03/15/example",
            "title": "RBC title",
            "publish_date_t": "2026-03-15T10:00:00+00:00",
            "announce": "Announce body",
            "authors": "bad-shape",
        },
        section="economics",
    )

    assert record is not None
    assert record["body"] == "Announce body"
    assert record["authors"] == []


def test_to_record_from_jsonld_maps_and_uses_title_when_body_missing() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))

    record = collector._to_record_from_jsonld(
        {
            "url": "https://www.rbc.ru/economics/2026/03/15/jsonld",
            "headline": "JSON-LD title",
            "datePublished": "2026-03-15T10:00:00+03:00",
            "author": [{"name": "A1"}, {"name": "A2"}],
        },
        section="economics",
    )

    assert record is not None
    assert record["body"] == "JSON-LD title"
    assert record["authors"] == ["A1", "A2"]
    assert str(record["published_at"]).endswith("+00:00")


def test_to_record_from_jsonld_returns_none_on_invalid_payload() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    assert collector._to_record_from_jsonld({"headline": "x"}, section="economics") is None
    assert (
        collector._to_record_from_jsonld(
            {"url": "https://www.rbc.ru/x", "headline": "x", "datePublished": "bad-date"},
            section="economics",
        )
        is None
    )


def test_parse_published_at_supports_multiple_formats() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))

    dt_from_iso = collector._parse_published_at({"publish_date_t": "2026-03-15T10:00:00+03:00"})
    dt_from_strftime = collector._parse_published_at({"publish_date": "15.03.2026 10:00"})
    dt_from_rfc2822 = collector._parse_published_at({"date": "Sun, 15 Mar 2026 10:00:00 +0300"})
    dt_invalid = collector._parse_published_at({"datetime": "not-a-date"})

    assert dt_from_iso is not None
    assert dt_from_strftime is not None
    assert dt_from_rfc2822 is not None
    assert dt_invalid is None


def test_get_article_data_handles_request_error() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._session.get = lambda url, timeout: (_ for _ in ()).throw(RuntimeError("boom"))  # type: ignore[method-assign]

    overview, text = collector._get_article_data("https://www.rbc.ru/economics/2026/03/15/example")
    assert overview is None
    assert text is None


def test_get_article_data_filters_unwanted_paragraphs() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    html = """
    <html><body>
      <div class="article__text__overview">Overview text</div>
      <p>Include this text</p>
      <div class="article__special_container"><p>Drop this text</p></div>
      <div class="showcase-collection__subtitle"><p>Drop too</p></div>
    </body></html>
    """

    class _Response:
        text = html

        def raise_for_status(self) -> None:
            return None

    collector._session.get = lambda url, timeout: _Response()  # type: ignore[method-assign]

    overview, text = collector._get_article_data("https://www.rbc.ru/economics/2026/03/15/example")
    assert overview == "Overview text"
    assert text == "Include this text"


def test_build_retry_uses_non_negative_values() -> None:
    retry = _build_retry(
        RBCCollectorConfig(
            sections=["economics"],
            max_retries=-3,
            backoff_seconds=-1.0,
        )
    )
    assert retry.total == 0
    assert retry.backoff_factor == 0.0


def test_collect_section_skips_none_records_from_search_items() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._fetch_search_items = lambda **kwargs: [  # type: ignore[method-assign]
        {"fronturl": "https://www.rbc.ru/1", "title": "ok", "publish_date_t": "2026-03-15T10:00:00+00:00"},
        {"fronturl": "https://www.rbc.ru/2", "title": "bad", "publish_date_t": "invalid"},
    ]
    collector._get_article_data = lambda url: (None, "body")  # type: ignore[method-assign]

    rows, failed = collector._collect_section("economics", "14.03.2026", "15.03.2026")

    assert failed is False
    assert len(rows) == 1
    assert rows[0]["url"] == "https://www.rbc.ru/1"


def test_collect_section_skips_duplicate_fallback_urls() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._fetch_search_items = lambda **kwargs: (_ for _ in ()).throw(RBCFetchError("boom"))  # type: ignore[method-assign]
    collector._fetch_fallback_section_records = lambda **kwargs: [  # type: ignore[method-assign]
        {"url": "https://www.rbc.ru/f1", "title": "T1", "body": "B1", "published_at": datetime.now(UTC), "authors": [], "section": "economics"},
        {"url": "https://www.rbc.ru/f1", "title": "T1 dup", "body": "B1", "published_at": datetime.now(UTC), "authors": [], "section": "economics"},
        {"url": "", "title": "empty", "body": "B", "published_at": datetime.now(UTC), "authors": [], "section": "economics"},
    ]

    rows, failed = collector._collect_section("economics", "14.03.2026", "15.03.2026")
    assert failed is False
    assert len(rows) == 1


def test_fetch_fallback_section_records_returns_empty_on_request_exception() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._session.get = lambda url, timeout: (_ for _ in ()).throw(RuntimeError("timeout"))  # type: ignore[method-assign]

    rows = collector._fetch_fallback_section_records("economics", "14.03.2026", "15.03.2026")
    assert rows == []


def test_fetch_fallback_section_records_skips_non_article_and_none_script_payload() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    html = """
    <html><body>
      <script type="application/ld+json"></script>
      <script type="application/ld+json">{"@type":"Thing","url":"https://www.rbc.ru/thing"}</script>
      <script type="application/ld+json">{"@type":"NewsArticle","url":"https://www.rbc.ru/a","headline":"A","datePublished":"2026-03-15T10:00:00+00:00"}</script>
    </body></html>
    """

    class _Response:
        text = html

        def raise_for_status(self) -> None:
            return None

    collector._session.get = lambda url, timeout: _Response()  # type: ignore[method-assign]
    collector._to_record_from_jsonld = lambda data, section: None if data.get("@type") == "NewsArticle" else {}  # type: ignore[method-assign]

    assert collector._fetch_fallback_section_records("economics", "14.03.2026", "15.03.2026") == []


def test_to_record_from_search_item_uses_title_when_all_bodies_empty() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    collector._get_article_data = lambda url: (None, None)  # type: ignore[method-assign]

    record = collector._to_record_from_search_item(
        {
            "fronturl": "https://www.rbc.ru/x",
            "title": "Fallback title",
            "publish_date_t": "2026-03-15T10:00:00+00:00",
            "announce": "   ",
        },
        section="economics",
    )
    assert record is not None
    assert record["body"] == "Fallback title"


def test_parse_published_at_handles_naive_datetime_overflow_and_empty_text() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))

    naive = datetime(2026, 3, 15, 10, 0)
    assert collector._parse_published_at({"publish_date_t": naive}) == naive.replace(tzinfo=UTC)
    assert collector._parse_published_at({"publish_date_t": 10**20, "publish_date": "2026-03-15T10:00:00+00:00"}) is not None
    assert collector._parse_published_at({"publish_date_t": "   ", "publish_date": "bad"}) is None


def test_parse_published_at_handles_rfc2822_without_tz() -> None:
    collector = RBCNewsCollector(RBCCollectorConfig(sections=["economics"], request_timeout=5, pages_limit=1))
    parsed = collector._parse_published_at({"date": "Sun, 15 Mar 2026 10:00:00"})
    assert parsed is not None
    assert parsed.tzinfo == UTC
