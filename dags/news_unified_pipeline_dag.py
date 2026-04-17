from __future__ import annotations

from datetime import timedelta
import logging
from typing import Any, Callable

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.dashboard.ner_metrics_job import run_ner_dashboard_metrics_job
from news_analyzer.pipeline.dedup.semantic_dedup_job import run_semantic_dedup_job
from news_analyzer.pipeline.enrich.ner_job import run_ner_job
from news_analyzer.pipeline.ingest.lenta_ingest import run_lenta_ingest
from news_analyzer.pipeline.ingest.rbc_ingest import run_rbc_ingest
from news_analyzer.pipeline.summarize.hourly_digest_job import run_hourly_digest_job
from news_analyzer.pipeline.summarize.item_summary_job import run_item_summary_job

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _run_ingest_safely(source: str, run_ingest: Callable[[], int]) -> dict[str, object]:
    try:
        created = run_ingest()
    except Exception as exc:  # noqa: BLE001
        logger.exception("%s ingest failed", source)
        return {
            "status": "failed",
            "source": source,
            "error": exc.__class__.__name__,
        }

    logger.info("%s ingest completed: created=%s", source, created)
    return {
        "status": "success",
        "source": source,
        "created": created,
    }


def run_rbc_ingest_safe() -> dict[str, object]:
    return _run_ingest_safely("rbc", run_rbc_ingest)


def run_lenta_ingest_safe() -> dict[str, object]:
    return _run_ingest_safely("lenta", run_lenta_ingest)


def _is_success(result: Any) -> bool:
    return isinstance(result, dict) and result.get("status") == "success"


def run_ingest_gate(ti) -> dict[str, object]:
    rbc_result = ti.xcom_pull(task_ids="rbc_ingest")
    lenta_result = ti.xcom_pull(task_ids="lenta_ingest")

    succeeded_sources: list[str] = []
    if _is_success(rbc_result):
        succeeded_sources.append("rbc")
    if _is_success(lenta_result):
        succeeded_sources.append("lenta")

    if not succeeded_sources:
        raise RuntimeError("Both ingest tasks failed; stopping unified pipeline run")

    logger.info("Ingest gate passed: succeeded_sources=%s", ",".join(succeeded_sources))
    return {
        "status": "success",
        "succeeded_sources": succeeded_sources,
    }


with DAG(
    dag_id="news_unified_pipeline",
    default_args=DEFAULT_ARGS,
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["news", "unified", "pipeline"],
) as dag:
    rbc_ingest_task = PythonOperator(task_id="rbc_ingest", python_callable=run_rbc_ingest_safe)
    lenta_ingest_task = PythonOperator(task_id="lenta_ingest", python_callable=run_lenta_ingest_safe)
    ingest_gate_task = PythonOperator(
        task_id="ingest_gate",
        python_callable=run_ingest_gate,
        trigger_rule="all_done",
    )

    dedup_task = PythonOperator(task_id="semantic_dedup", python_callable=run_semantic_dedup_job)
    nlp_task = PythonOperator(task_id="ner_and_classification", python_callable=run_ner_job)
    item_summaries_task = PythonOperator(task_id="item_summaries", python_callable=run_item_summary_job)
    hourly_digest_task = PythonOperator(task_id="hourly_digest", python_callable=run_hourly_digest_job)
    dashboard_task = PythonOperator(
        task_id="refresh_ner_entity_metrics",
        python_callable=run_ner_dashboard_metrics_job,
    )

    [rbc_ingest_task, lenta_ingest_task] >> ingest_gate_task
    ingest_gate_task >> dedup_task >> nlp_task
    nlp_task >> item_summaries_task >> hourly_digest_task
    nlp_task >> dashboard_task

__all__ = ["dag"]
