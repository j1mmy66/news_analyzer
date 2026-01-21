from __future__ import annotations

from news_analyzer.pipeline.summarize.item_summary_job import run_item_summary_job


def run_retry_missing_summaries_job(limit: int = 200) -> int:
    return run_item_summary_job(limit=limit)
