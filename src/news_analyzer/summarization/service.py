from __future__ import annotations

from datetime import datetime, timezone

from news_analyzer.domain.enums import ProcessingStatus
from news_analyzer.domain.models import SummaryResult
from news_analyzer.summarization.gigachat.cache import InMemorySummaryCache
from news_analyzer.summarization.gigachat.client import GigaChatClient
from news_analyzer.summarization.gigachat.mapper import build_hourly_prompt, build_item_prompt


class SummaryService:
    def __init__(self, client: GigaChatClient, cache: InMemorySummaryCache | None = None) -> None:
        self._client = client
        self._cache = cache or InMemorySummaryCache()

    def summarize_item(self, text: str) -> SummaryResult:
        if not text.strip():
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code="EMPTY_TEXT",
                updated_at=datetime.now(timezone.utc),
            )

        cached = self._cache.get(text)
        if cached is not None:
            return SummaryResult(
                summary=cached,
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )

        try:
            summary = self._client.summarize(build_item_prompt(text))
            self._cache.set(text, summary)
            return SummaryResult(
                summary=summary,
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )
        except Exception as exc:  # noqa: BLE001
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code=exc.__class__.__name__,
                updated_at=datetime.now(timezone.utc),
            )

    def summarize_hour(self, texts: list[str]) -> SummaryResult:
        if not texts:
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code="EMPTY_BATCH",
                updated_at=datetime.now(timezone.utc),
            )

        combined = "\n".join(texts)
        cached = self._cache.get(combined)
        if cached is not None:
            return SummaryResult(
                summary=cached,
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )

        try:
            summary = self._client.summarize(build_hourly_prompt(texts))
            self._cache.set(combined, summary)
            return SummaryResult(
                summary=summary,
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )
        except Exception as exc:  # noqa: BLE001
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code=exc.__class__.__name__,
                updated_at=datetime.now(timezone.utc),
            )
