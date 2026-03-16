from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.summarize.retry_missing_summaries_job import run_retry_missing_summaries_job

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="news_retry_missing_summaries",
    default_args=DEFAULT_ARGS,
    schedule="0 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["summaries", "retry", "news"],
) as dag:
    PythonOperator(task_id="retry_missing_summaries", python_callable=run_retry_missing_summaries_job)

__all__ = ["dag"]
