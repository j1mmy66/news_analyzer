from datetime import datetime, timezone

from news_analyzer.domain.enums import ClassLabel, ProcessingStatus, SourceType
from news_analyzer.domain.models import ClassificationResult, Entity, HourlyDigest, NormalizedNewsItem, SummaryResult


def test_domain_schemas_construct() -> None:
    item = NormalizedNewsItem(
        source_type=SourceType.RBC,
        external_id="rbc:https://www.rbc.ru/economics/test",
        published_at=datetime.now(timezone.utc),
        source_metadata={"url": "https://www.rbc.ru/economics/test"},
        raw_text="raw",
        cleaned_text="clean",
    )
    entity = Entity(text="Moscow", label="LOC", start=0, end=6, confidence=0.9, normalized="moscow")
    cls = ClassificationResult(class_label=ClassLabel.OTHER, class_confidence=0.5, model_version="v1")
    summary = SummaryResult(summary="ok", status=ProcessingStatus.SUCCESS, error_code=None, updated_at=datetime.now(timezone.utc))
    digest = HourlyDigest(
        digest_id="digest-1",
        window_start=datetime.now(timezone.utc),
        window_end=datetime.now(timezone.utc),
        summary="digest",
        news_ids=[item.external_id],
    )

    assert item.external_id in digest.news_ids
    assert entity.label == "LOC"
    assert cls.model_version == "v1"
    assert summary.status == ProcessingStatus.SUCCESS


def test_domain_schemas_support_lenta_source() -> None:
    item = NormalizedNewsItem(
        source_type=SourceType.LENTA,
        external_id="lenta:https://lenta.ru/news/2026/03/17/123",
        published_at=datetime.now(timezone.utc),
        source_metadata={"url": "https://lenta.ru/news/2026/03/17/123", "section": "Россия"},
        raw_text="raw",
        cleaned_text="clean",
    )

    assert item.source_type == SourceType.LENTA
    assert item.external_id.startswith("lenta:")
