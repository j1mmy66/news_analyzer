from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class NewsCursor:
    published_at: str
    external_id: str

    def to_search_after(self) -> list[str]:
        return [self.published_at, self.external_id]

    @classmethod
    def from_sort(cls, sort: list[object] | tuple[object, ...]) -> "NewsCursor | None":
        if len(sort) < 2:
            return None
        published_at, external_id = sort[0], sort[1]
        if not isinstance(published_at, str) or not isinstance(external_id, str):
            return None
        return cls(published_at=published_at, external_id=external_id)


@dataclass(frozen=True)
class NewsCard:
    external_id: str
    title: str
    summary: str | None
    class_label: str | None
    published_at: datetime | None
    source_type: str | None
    raw_text: str | None
    url: str | None
    authors: str
    section: str | None


@dataclass(frozen=True)
class NewsPage:
    items: list[NewsCard]
    next_cursor: NewsCursor | None
    has_more: bool


@dataclass(frozen=True)
class HourlyDigestView:
    digest_id: str
    window_start: datetime
    window_end: datetime
    summary: str | None
    news_count: int
