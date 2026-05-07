from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from news_analyzer.domain.enums import ClassLabel, SourceType
from news_analyzer.domain.models import ClassificationResult, Entity, NormalizedNewsItem
from news_analyzer.pipeline.dashboard import ner_metrics_job
from news_analyzer.pipeline.dashboard.ner_metrics_job import EntityMetricRow
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.repositories import NewsRepository


def _fetch_table_rows(pg_conn_kwargs: dict[str, object], table_name: str) -> list[tuple[object, ...]]:
    with psycopg.connect(**pg_conn_kwargs) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT entity_name, entity_type, count_3h, count_24h
                FROM {table_name}
                ORDER BY entity_name, entity_type
                """
            )
            return cursor.fetchall()


def _refresh(rows: list[EntityMetricRow], pg_conn_kwargs: dict[str, object], table_name: str) -> int:
    return ner_metrics_job._refresh_metrics_table(
        rows=rows,
        host=str(pg_conn_kwargs["host"]),
        port=int(pg_conn_kwargs["port"]),
        database=str(pg_conn_kwargs["dbname"]),
        user=str(pg_conn_kwargs["user"]),
        password=str(pg_conn_kwargs["password"]),
        table_name=table_name,
    )


def _sample_item(external_id: str, published_at: datetime) -> NormalizedNewsItem:
    return NormalizedNewsItem(
        source_type=SourceType.RBC,
        external_id=external_id,
        published_at=published_at,
        source_metadata={"url": f"https://example.com/{external_id}"},
        raw_text=f"raw-{external_id}",
        cleaned_text=f"text-{external_id}",
    )


def test_refresh_metrics_table_success_truncates_and_replaces_rows(pg_conn_kwargs, pg_table_cleanup) -> None:
    table_name = pg_table_cleanup
    rows_v1 = [
        EntityMetricRow(
            entity_name="москва",
            entity_type="LOC",
            count_3h=1,
            count_24h=2,
            last_seen_at=datetime.now(timezone.utc),
        )
    ]
    inserted_v1 = _refresh(rows_v1, pg_conn_kwargs, table_name)
    assert inserted_v1 == 1
    assert _fetch_table_rows(pg_conn_kwargs, table_name) == [("москва", "LOC", 1, 2)]

    rows_v2 = [
        EntityMetricRow(
            entity_name="сбер",
            entity_type="ORG",
            count_3h=2,
            count_24h=2,
            last_seen_at=datetime.now(timezone.utc),
        ),
        EntityMetricRow(
            entity_name="иван иванов",
            entity_type="PER",
            count_3h=1,
            count_24h=3,
            last_seen_at=datetime.now(timezone.utc),
        ),
    ]
    inserted_v2 = _refresh(rows_v2, pg_conn_kwargs, table_name)
    assert inserted_v2 == 2
    assert _fetch_table_rows(pg_conn_kwargs, table_name) == [
        ("иван иванов", "PER", 1, 3),
        ("сбер", "ORG", 2, 2),
    ]


def test_refresh_metrics_table_rolls_back_when_insert_fails(pg_conn_kwargs, pg_table_cleanup) -> None:
    table_name = pg_table_cleanup
    baseline = [
        EntityMetricRow(
            entity_name="москва",
            entity_type="LOC",
            count_3h=1,
            count_24h=2,
            last_seen_at=datetime.now(timezone.utc),
        )
    ]
    _refresh(baseline, pg_conn_kwargs, table_name)
    assert _fetch_table_rows(pg_conn_kwargs, table_name) == [("москва", "LOC", 1, 2)]

    broken_rows = [
        EntityMetricRow(
            entity_name="dup",
            entity_type="ORG",
            count_3h=1,
            count_24h=1,
            last_seen_at=datetime.now(timezone.utc),
        ),
        EntityMetricRow(
            entity_name="dup",
            entity_type="ORG",
            count_3h=2,
            count_24h=2,
            last_seen_at=datetime.now(timezone.utc),
        ),
    ]
    with pytest.raises(psycopg.Error):
        _refresh(broken_rows, pg_conn_kwargs, table_name)

    # TRUNCATE and failed INSERT run in one transaction; baseline data must survive rollback.
    assert _fetch_table_rows(pg_conn_kwargs, table_name) == [("москва", "LOC", 1, 2)]


def test_run_ner_dashboard_metrics_job_e2e(monkeypatch, opensearch_client, indexed_os_names, pg_conn_kwargs, pg_table_cleanup) -> None:
    news_index = indexed_os_names["news_index"]
    now = datetime.now(timezone.utc)
    repository = NewsRepository(opensearch_client, news_index)

    assert repository.upsert_news(
        [
            _sample_item("n1", now - timedelta(hours=1)),
            _sample_item("n2", now - timedelta(hours=4)),
            _sample_item("n3", now - timedelta(hours=2)),
        ]
    ) == 3
    repository.set_enrichment(
        "n1",
        entities=[
            Entity(text="Москва", label="LOC", start=0, end=6, confidence=0.9, normalized="москва"),
            Entity(text="РБК", label="ORG", start=7, end=10, confidence=0.7, normalized="РБК"),
        ],
        classification=ClassificationResult(ClassLabel.ECONOMY, 0.7, "it"),
    )
    repository.set_enrichment(
        "n2",
        entities=[Entity(text="Москва", label="LOC", start=0, end=6, confidence=0.8, normalized="москва")],
        classification=ClassificationResult(ClassLabel.ECONOMY, 0.65, "it"),
    )
    repository.set_enrichment(
        "n3",
        entities=[Entity(text="MAX", label="ORG", start=0, end=3, confidence=0.8, normalized="MAX")],
        classification=ClassificationResult(ClassLabel.ECONOMY, 0.6, "it"),
    )
    opensearch_client.indices.refresh(index=news_index)

    settings = AppSettings(
        opensearch_hosts=["http://localhost:19200"],
        opensearch_news_index=news_index,
        opensearch_digests_index=indexed_os_names["digests_index"],
        opensearch_username=None,
        opensearch_password=None,
        opensearch_use_ssl=False,
        opensearch_verify_certs=False,
        dashboard_pg_host=str(pg_conn_kwargs["host"]),
        dashboard_pg_port=int(pg_conn_kwargs["port"]),
        dashboard_pg_database=str(pg_conn_kwargs["dbname"]),
        dashboard_pg_user=str(pg_conn_kwargs["user"]),
        dashboard_pg_password=str(pg_conn_kwargs["password"]),
        dashboard_pg_table=pg_table_cleanup,
    )
    monkeypatch.setattr(ner_metrics_job.AppSettings, "from_env", classmethod(lambda cls: settings))

    written = ner_metrics_job.run_ner_dashboard_metrics_job(limit=5000)
    assert written == 1
    assert _fetch_table_rows(pg_conn_kwargs, pg_table_cleanup) == [("москва", "LOC", 1, 2)]
