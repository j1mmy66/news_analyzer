from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

from news_analyzer.pipeline.enrich.ner_job import run_ner_job

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="news_nlp_enrichment",
    default_args=DEFAULT_ARGS,
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["nlp", "enrichment", "news"],
) as dag:
    PythonOperator(task_id="ner_and_classification", python_callable=run_ner_job)

__all__ = ["dag"]
