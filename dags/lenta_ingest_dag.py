from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.ingest.lenta_ingest import run_lenta_ingest

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lenta_news_ingest",
    default_args=DEFAULT_ARGS,
    schedule="*/20 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["lenta", "ingest", "news"],
) as dag:
    PythonOperator(task_id="lenta_ingest", python_callable=run_lenta_ingest)

__all__ = ["dag"]
