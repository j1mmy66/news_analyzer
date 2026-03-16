from __future__ import annotations

from news_analyzer.pipeline.enrich.ner_job import run_ner_job


def run_classify_job(limit: int = 300) -> int:
    # Classification currently runs together with NER to keep one enrichment pass.
    return run_ner_job(limit=limit)
