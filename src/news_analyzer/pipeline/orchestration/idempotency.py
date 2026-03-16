from __future__ import annotations

from news_analyzer.domain.enums import SourceType


def idempotency_key(source_type: SourceType, external_id: str) -> str:
    return f"{source_type.value}:{external_id}"
