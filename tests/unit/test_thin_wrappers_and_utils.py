from __future__ import annotations

from datetime import datetime, timezone
import logging

from news_analyzer.domain.enums import SourceType
from news_analyzer.observability.logging import configure_logging
from news_analyzer.observability.metrics import PipelineMetrics
from news_analyzer.pipeline.enrich import classify_job
from news_analyzer.pipeline.orchestration.idempotency import idempotency_key
from news_analyzer.pipeline.orchestration.run_context import RunContext
from news_analyzer.pipeline.summarize import retry_missing_summaries_job


def test_run_classify_job_delegates_to_ner_job(monkeypatch) -> None:
    captured: dict[str, int] = {}

    def _fake_run_ner_job(limit: int = 300) -> int:
        captured["limit"] = limit
        return 17

    monkeypatch.setattr(classify_job, "run_ner_job", _fake_run_ner_job)

    assert classify_job.run_classify_job(limit=123) == 17
    assert captured["limit"] == 123


def test_run_retry_missing_summaries_job_delegates_to_item_summary_job(monkeypatch) -> None:
    captured: dict[str, int] = {}

    def _fake_run_item_summary_job(limit: int = 200) -> int:
        captured["limit"] = limit
        return 9

    monkeypatch.setattr(retry_missing_summaries_job, "run_item_summary_job", _fake_run_item_summary_job)

    assert retry_missing_summaries_job.run_retry_missing_summaries_job(limit=77) == 9
    assert captured["limit"] == 77


def test_idempotency_key_uses_source_type_and_external_id() -> None:
    assert idempotency_key(SourceType.RBC, "abc-42") == "rbc:abc-42"
    assert idempotency_key(SourceType.LENTA, "abc-42") == "lenta:abc-42"


def test_run_context_create_sets_uuid_and_utc_timestamp(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 17, 8, 15, tzinfo=timezone.utc)

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return fixed_now

    class _UUIDValue:
        def __str__(self) -> str:
            return "00000000-0000-0000-0000-000000000123"

    monkeypatch.setattr("news_analyzer.pipeline.orchestration.run_context.datetime", _FakeDatetime)
    monkeypatch.setattr("news_analyzer.pipeline.orchestration.run_context.uuid.uuid4", lambda: _UUIDValue())

    context = RunContext.create()

    assert context.run_id == "00000000-0000-0000-0000-000000000123"
    assert context.started_at == fixed_now


def test_configure_logging_calls_basic_config(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_basic_config(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(logging, "basicConfig", _fake_basic_config)

    configure_logging(level=logging.DEBUG)

    assert captured["level"] == logging.DEBUG
    assert captured["format"] == "%(asctime)s %(levelname)s %(name)s :: %(message)s"


def test_pipeline_metrics_defaults_and_assignment() -> None:
    metrics = PipelineMetrics()
    assert metrics.ingested == 0
    assert metrics.parsed_failed == 0
    assert metrics.enriched_failed == 0
    assert metrics.summarized_failed == 0

    metrics.ingested = 5
    metrics.parsed_failed = 1
    metrics.enriched_failed = 2
    metrics.summarized_failed = 3

    assert metrics.ingested == 5
    assert metrics.parsed_failed == 1
    assert metrics.enriched_failed == 2
    assert metrics.summarized_failed == 3
