from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from news_analyzer.pipeline.dashboard import ner_metrics_job
from news_analyzer.pipeline.dashboard.ner_metrics_job import EntityMetricRow
from news_analyzer.settings.app_settings import AppSettings


def test_aggregate_entity_metrics_uses_normalized_with_text_fallback_and_counts_mentions() -> None:
    now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    items = [
        {
            "published_at": "2026-03-15T11:00:00+00:00",
            "entities": [
                {"normalized": "иван иванов", "text": "Иван Иванов", "label": "PER"},
                {"normalized": "", "text": "Сбер", "label": "ORG"},
                {"normalized": "иван иванов", "text": "Иван Иванов", "label": "PER"},
            ],
        },
        {
            "published_at": "2026-03-15T08:30:00+00:00",
            "entities": [{"normalized": "иван иванов", "text": "Иван Иванов", "label": "PER"}],
        },
        {
            "published_at": "2026-03-14T13:30:00+00:00",
            "entities": [{"normalized": "иван иванов", "text": "Иван Иванов", "label": "PER"}],
        },
    ]

    rows = ner_metrics_job._aggregate_entity_metrics(items, now_utc=now_utc)
    by_key = {(row.entity_name, row.entity_type): row for row in rows}

    per_row = by_key[("иван иванов", "PER")]
    assert per_row.count_3h == 2
    assert per_row.count_24h == 4
    assert per_row.last_seen_at == datetime(2026, 3, 15, 11, 0, tzinfo=timezone.utc)

    org_row = by_key[("Сбер", "ORG")]
    assert org_row.count_3h == 1
    assert org_row.count_24h == 1


def test_aggregate_entity_metrics_uses_utc_for_naive_timestamps() -> None:
    now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    items = [
        {
            "published_at": "2026-03-15T10:00:00",
            "entities": [{"normalized": "москва", "text": "Москва", "label": "LOC"}],
        }
    ]

    rows = ner_metrics_job._aggregate_entity_metrics(items, now_utc=now_utc)
    assert rows == [
        EntityMetricRow(
            entity_name="москва",
            entity_type="LOC",
            count_3h=1,
            count_24h=1,
            last_seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
        )
    ]


def test_aggregate_entity_metrics_excludes_configured_entities() -> None:
    now_utc = datetime(2026, 3, 15, 12, 0, tzinfo=timezone.utc)
    items = [
        {
            "published_at": "2026-03-15T11:00:00+00:00",
            "entities": [
                {"normalized": "РБК", "text": "РБК", "label": "ORG"},
                {"normalized": "max", "text": "Max", "label": "ORG"},
                {"normalized": "maх", "text": "MAХ", "label": "ORG"},
                {"normalized": "москва", "text": "Москва", "label": "LOC"},
            ],
        }
    ]

    rows = ner_metrics_job._aggregate_entity_metrics(items, now_utc=now_utc)
    assert rows == [
        EntityMetricRow(
            entity_name="москва",
            entity_type="LOC",
            count_3h=1,
            count_24h=1,
            last_seen_at=datetime(2026, 3, 15, 11, 0, tzinfo=timezone.utc),
        )
    ]


class _CursorStub:
    def __init__(self) -> None:
        self.executed: list[object] = []
        self.bulk_rows: list[tuple[object, list[tuple[object, ...]]]] = []

    def execute(self, query: object, *args: object, **kwargs: object) -> None:
        self.executed.append(query)

    def executemany(self, query: object, rows: list[tuple[object, ...]]) -> None:
        self.bulk_rows.append((query, rows))

    def __enter__(self) -> "_CursorStub":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _ConnectionStub:
    def __init__(self) -> None:
        self.cursor_stub = _CursorStub()
        self.committed = False

    def cursor(self) -> _CursorStub:
        return self.cursor_stub

    def commit(self) -> None:
        self.committed = True

    def __enter__(self) -> "_ConnectionStub":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


def test_refresh_metrics_table_creates_table_and_replaces_rows(monkeypatch) -> None:
    conn_stub = _ConnectionStub()
    monkeypatch.setattr(ner_metrics_job, "_connect_postgres", lambda **kwargs: conn_stub)
    rows = [
        EntityMetricRow(
            entity_name="москва",
            entity_type="LOC",
            count_3h=2,
            count_24h=5,
            last_seen_at=datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc),
        )
    ]

    inserted = ner_metrics_job._refresh_metrics_table(
        rows=rows,
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow",
        table_name="ner_entity_metrics",
    )

    assert inserted == 1
    assert len(conn_stub.cursor_stub.executed) == 2
    assert len(conn_stub.cursor_stub.bulk_rows) == 1
    assert conn_stub.cursor_stub.bulk_rows[0][1] == [
        ("москва", "LOC", 2, 5, datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc))
    ]
    assert conn_stub.committed is True


def test_run_ner_dashboard_metrics_job_end_to_end_with_stubs(tmp_path: Path, monkeypatch) -> None:
    settings = AppSettings(
        opensearch_hosts=["http://localhost:9200"],
        dashboard_pg_host="postgres",
        dashboard_pg_port=5432,
        dashboard_pg_database="airflow",
        dashboard_pg_user="airflow",
        dashboard_pg_password="airflow",
        dashboard_pg_table="ner_entity_metrics",
        classifier_model_path=tmp_path / "any-news-classifier",
        ner_slovnet_model_path=tmp_path / "slovnet.tar",
        ner_navec_path=tmp_path / "navec.tar",
    )
    settings.classifier_model_path.mkdir(parents=True, exist_ok=True)
    settings.ner_slovnet_model_path.write_text("model", encoding="utf-8")
    settings.ner_navec_path.write_text("model", encoding="utf-8")

    class _RepoStub:
        def get_news_for_last_hours(self, hours: int, now: datetime, limit: int) -> list[dict[str, object]]:
            assert hours == 24
            assert limit == 5000
            return [
                {
                    "published_at": "2026-03-15T11:00:00+00:00",
                    "entities": [{"normalized": "москва", "text": "Москва", "label": "LOC"}],
                }
            ]

    monkeypatch.setattr(ner_metrics_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_metrics_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_metrics_job, "NewsRepository", lambda _client, _index: _RepoStub())
    monkeypatch.setattr(ner_metrics_job, "_refresh_metrics_table", lambda **kwargs: len(kwargs["rows"]))

    written = ner_metrics_job.run_ner_dashboard_metrics_job()
    assert written == 1
