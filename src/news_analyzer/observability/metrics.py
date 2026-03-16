from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PipelineMetrics:
    ingested: int = 0
    parsed_failed: int = 0
    enriched_failed: int = 0
    summarized_failed: int = 0
