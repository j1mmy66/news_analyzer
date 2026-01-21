from datetime import datetime, timezone

from news_analyzer.sources.rbc.parser import parse_rbc_article


def test_rbc_parser_required_fields() -> None:
    item = parse_rbc_article(
        {
            "url": "https://www.rbc.ru/economics/01/01/2026/abc",
            "title": "Title",
            "body": "Body",
            "published_at": datetime.now(timezone.utc),
            "authors": ["Author"],
            "section": "economics",
        }
    )
    assert item.source_metadata["url"].startswith("https://")
    assert item.source_metadata["section"] == "economics"
