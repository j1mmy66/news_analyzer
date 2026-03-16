from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.ingest.rbc_ingest import run_rbc_ingest

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="rbc_news_ingest",
    default_args=DEFAULT_ARGS,
    schedule="*/20 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["rbc", "ingest", "news"],
) as dag:
    PythonOperator(task_id="rbc_ingest", python_callable=run_rbc_ingest)

__all__ = ["dag"]
