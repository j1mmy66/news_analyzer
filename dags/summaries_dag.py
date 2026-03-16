from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.summarize.hourly_digest_job import run_hourly_digest_job
from news_analyzer.pipeline.summarize.item_summary_job import run_item_summary_job

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="news_summaries",
    default_args=DEFAULT_ARGS,
    schedule="*/30 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["summaries", "news"],
) as dag:
    item_task = PythonOperator(task_id="item_summaries", python_callable=run_item_summary_job)
    digest_task = PythonOperator(task_id="hourly_digest", python_callable=run_hourly_digest_job)

    item_task >> digest_task

__all__ = ["dag"]
