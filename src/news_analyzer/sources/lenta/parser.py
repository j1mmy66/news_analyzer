from __future__ import annotations

from datetime import timezone
from typing import Any

from news_analyzer.domain.enums import SourceType
from news_analyzer.domain.models import NormalizedNewsItem


class LentaParseError(ValueError):
    pass


def parse_lenta_article(raw: dict[str, Any]) -> NormalizedNewsItem:
    url = raw.get("url")
    title = (raw.get("title") or "").strip()
    body = (raw.get("body") or "").strip()
    published_at = raw.get("published_at")
    authors = raw.get("authors") or []
    section = (raw.get("section") or "").strip()

    if not url or not published_at or not section:
        raise LentaParseError("Missing required Lenta fields")

    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=timezone.utc)

    raw_text = "\n\n".join([part for part in [title, body] if part])

    return NormalizedNewsItem(
        source_type=SourceType.LENTA,
        external_id=f"lenta:{url}",
        published_at=published_at.astimezone(timezone.utc),
        source_metadata={
            "url": str(url),
            "title": title,
            "body": body,
            "published_at": published_at.astimezone(timezone.utc).isoformat(),
            "authors": [str(value) for value in authors if str(value).strip()] if isinstance(authors, list) else [],
            "section": section,
        },
        raw_text=raw_text,
        cleaned_text=" ".join(raw_text.split()),
    )
