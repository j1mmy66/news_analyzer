from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from news_analyzer.domain.enums import ClassLabel, ProcessingStatus, SourceType


@dataclass(frozen=True)
class NormalizedNewsItem:
    source_type: SourceType
    external_id: str
    published_at: datetime
    source_metadata: dict[str, Any]
    raw_text: str
    cleaned_text: str


@dataclass(frozen=True)
class Entity:
    text: str
    label: str
    start: int
    end: int
    confidence: float
    normalized: str | None = None


@dataclass(frozen=True)
class ClassificationResult:
    class_label: ClassLabel
    class_confidence: float
    model_version: str


@dataclass(frozen=True)
class SummaryResult:
    summary: str | None
    status: ProcessingStatus
    error_code: str | None
    updated_at: datetime


@dataclass(frozen=True)
class HourlyDigest:
    digest_id: str
    window_start: datetime
    window_end: datetime
    summary: str
    news_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DedupMetadataUpdate:
    external_id: str
    is_canonical: bool
    canonical_external_id: str
    similarity_to_canonical: float
