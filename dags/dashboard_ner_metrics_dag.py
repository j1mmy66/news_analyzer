from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.dashboard.ner_metrics_job import run_ner_dashboard_metrics_job

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dashboard_ner_metrics",
    default_args=DEFAULT_ARGS,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dashboard", "superset", "ner"],
) as dag:
    PythonOperator(task_id="refresh_ner_entity_metrics", python_callable=run_ner_dashboard_metrics_job)

__all__ = ["dag"]

