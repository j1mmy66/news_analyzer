from __future__ import annotations

from datetime import datetime, timezone

import pytest

from news_analyzer.sources.rbc.parser import RBCParseError, parse_rbc_article


def test_parse_rbc_article_raises_on_missing_required_fields() -> None:
    with pytest.raises(RBCParseError, match="Missing required RBC fields"):
        parse_rbc_article({"url": "https://www.rbc.ru/x", "title": "x", "body": "x"})


def test_parse_rbc_article_assigns_utc_for_naive_published_at() -> None:
    raw = {
        "url": "https://www.rbc.ru/economics/01/01/2026/abc",
        "title": "Title",
        "body": "Body",
        "published_at": datetime(2026, 1, 1, 10, 0),
        "authors": ["Author"],
        "section": "economics",
    }

    item = parse_rbc_article(raw)

    assert item.published_at.tzinfo == timezone.utc
    assert item.source_metadata["published_at"].endswith("+00:00")
