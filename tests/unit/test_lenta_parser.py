from __future__ import annotations

from datetime import UTC, datetime

import pytest

from news_analyzer.domain.enums import SourceType
from news_analyzer.sources.lenta.parser import LentaParseError, parse_lenta_article


def test_parse_lenta_article_raises_on_missing_required_fields() -> None:
    with pytest.raises(LentaParseError, match="Missing required Lenta fields"):
        parse_lenta_article({"url": "https://lenta.ru/news/1", "title": "x"})


def test_parse_lenta_article_assigns_utc_for_naive_published_at() -> None:
    raw = {
        "url": "https://lenta.ru/news/2026/01/01/example",
        "title": "Title",
        "body": "Body",
        "published_at": datetime(2026, 1, 1, 10, 0),
        "authors": [],
        "section": "Россия",
    }

    item = parse_lenta_article(raw)

    assert item.source_type == SourceType.LENTA
    assert item.external_id == "lenta:https://lenta.ru/news/2026/01/01/example"
    assert item.published_at.tzinfo == UTC
    assert item.source_metadata["published_at"].endswith("+00:00")
    assert item.source_metadata["section"] == "Россия"
