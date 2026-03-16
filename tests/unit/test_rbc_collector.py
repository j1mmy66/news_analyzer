from datetime import UTC, datetime

from news_analyzer.sources.rbc.collector import RBCFetchError, RBCNewsCollector
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

    published_at = collector._parse_published_at({"publish_date_t": 1_773_571_200_000})
    assert published_at == datetime(2026, 3, 15, 0, 0, tzinfo=UTC)


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
