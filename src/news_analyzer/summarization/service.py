from __future__ import annotations

from datetime import datetime, timezone
import logging

from news_analyzer.domain.enums import ProcessingStatus
from news_analyzer.domain.models import SummaryResult
from news_analyzer.pipeline.orchestration.text_preprocessor import prepare_hourly_texts, truncate_text
from news_analyzer.summarization.gigachat.cache import InMemorySummaryCache
from news_analyzer.summarization.gigachat.client import GigaChatClient
from news_analyzer.summarization.gigachat.mapper import build_hourly_prompt, build_item_prompt

logger = logging.getLogger(__name__)


class SummaryService:
    def __init__(
        self,
        client: GigaChatClient,
        cache: InMemorySummaryCache | None = None,
        *,
        item_text_max_chars: int = 5000,
        hourly_item_text_max_chars: int = 1500,
        hourly_total_text_max_chars: int = 10000,
        hourly_latest_first: bool = True,
    ) -> None:
        self._client = client
        self._cache = cache or InMemorySummaryCache()
        self._item_text_max_chars = item_text_max_chars
        self._hourly_item_text_max_chars = hourly_item_text_max_chars
        self._hourly_total_text_max_chars = hourly_total_text_max_chars
        self._hourly_latest_first = hourly_latest_first

    def summarize_item(self, text: str) -> SummaryResult:
        if not text.strip():
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code="EMPTY_TEXT",
                updated_at=datetime.now(timezone.utc),
            )

        prepared = truncate_text(text, self._item_text_max_chars)
        if prepared.was_truncated:
            logger.info(
                "Summary text preprocess: text_truncated=%s truncated_count=%s dropped_count=%s input_chars=%s output_chars=%s limit_chars=%s",
                prepared.was_truncated,
                prepared.truncated_count,
                prepared.dropped_count,
                prepared.input_chars,
                prepared.output_chars,
                self._item_text_max_chars,
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
            summary = self._client.summarize(build_item_prompt(prepared.text))
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

        prepared = prepare_hourly_texts(
            texts,
            per_item_max_chars=self._hourly_item_text_max_chars,
            total_max_chars=self._hourly_total_text_max_chars,
            latest_first=self._hourly_latest_first,
        )
        if prepared.was_truncated:
            logger.info(
                "Summary batch preprocess: text_truncated=%s truncated_count=%s dropped_count=%s input_chars=%s output_chars=%s per_item_limit=%s total_limit=%s",
                prepared.was_truncated,
                prepared.truncated_count,
                prepared.dropped_count,
                prepared.input_chars,
                prepared.output_chars,
                self._hourly_item_text_max_chars,
                self._hourly_total_text_max_chars,
            )
        if not prepared.texts:
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code="EMPTY_BATCH",
                updated_at=datetime.now(timezone.utc),
            )

        combined = "\n".join(prepared.texts)
        cached = self._cache.get(combined)
        if cached is not None:
            return SummaryResult(
                summary=cached,
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )

        try:
            summary = self._client.summarize(build_hourly_prompt(prepared.texts))
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
