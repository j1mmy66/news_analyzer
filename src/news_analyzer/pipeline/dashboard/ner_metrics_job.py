from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any

from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.repositories import NewsRepository

logger = logging.getLogger(__name__)
_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXCLUDED_ENTITY_NAMES = {"РБК", "MAX", "MAХ"}


@dataclass(frozen=True)
class EntityMetricRow:
    entity_name: str
    entity_type: str
    count_3h: int
    count_24h: int
    last_seen_at: datetime


def _to_utc(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _normalize_entity_name(entity: dict[str, Any]) -> str:
    normalized = str(entity.get("normalized") or "").strip()
    if normalized:
        return normalized
    return str(entity.get("text") or "").strip()


def _is_excluded_entity(entity_name: str) -> bool:
    return entity_name.strip().upper() in _EXCLUDED_ENTITY_NAMES


def _aggregate_entity_metrics(items: list[dict[str, object]], now_utc: datetime) -> list[EntityMetricRow]:
    window_3h_start = now_utc - timedelta(hours=3)
    window_24h_start = now_utc - timedelta(hours=24)

    counts_24h: dict[tuple[str, str], int] = defaultdict(int)
    counts_3h: dict[tuple[str, str], int] = defaultdict(int)
    last_seen: dict[tuple[str, str], datetime] = {}

    for item in items:
        published_at = _to_utc(item.get("published_at"))  # type: ignore[arg-type]
        if published_at is None or published_at < window_24h_start:
            continue

        entities = item.get("entities")
        if not isinstance(entities, list):
            continue

        for raw in entities:
            if not isinstance(raw, dict):
                continue
            entity_name = _normalize_entity_name(raw)
            entity_type = str(raw.get("label") or "").strip()
            if not entity_name or not entity_type or _is_excluded_entity(entity_name):
                continue

            key = (entity_name, entity_type)
            counts_24h[key] += 1
            if published_at >= window_3h_start:
                counts_3h[key] += 1
            if key not in last_seen or published_at > last_seen[key]:
                last_seen[key] = published_at

    rows = [
        EntityMetricRow(
            entity_name=key[0],
            entity_type=key[1],
            count_3h=counts_3h.get(key, 0),
            count_24h=count_24h,
            last_seen_at=last_seen[key],
        )
        for key, count_24h in counts_24h.items()
    ]
    rows.sort(key=lambda row: (row.count_24h, row.count_3h, row.last_seen_at), reverse=True)
    return rows


def _connect_postgres(*, host: str, port: int, database: str, user: str, password: str):
    try:
        import psycopg
    except ModuleNotFoundError as exc:  # pragma: no cover - guarded by runtime environment
        raise RuntimeError("psycopg is required for dashboard metrics refresh") from exc

    return psycopg.connect(
        host=host,
        port=port,
        dbname=database,
        user=user,
        password=password,
        autocommit=False,
    )


def _table_sql(table_name: str, template: str):
    if not _TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(f"Invalid table name: {table_name}")
    try:
        from psycopg import sql
    except ModuleNotFoundError:
        return template.format(table_name=table_name)
    return sql.SQL(template).format(table_name=sql.Identifier(table_name))


def _ensure_table(cursor: Any, table_name: str) -> None:
    cursor.execute(
        _table_sql(
            table_name,
            """
            CREATE TABLE IF NOT EXISTS {table_name} (
                entity_name TEXT NOT NULL,
                entity_type TEXT NOT NULL,
                count_3h INTEGER NOT NULL,
                count_24h INTEGER NOT NULL,
                last_seen_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (entity_name, entity_type)
            )
            """,
        )
    )


def _refresh_metrics_table(
    *,
    rows: list[EntityMetricRow],
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    table_name: str,
) -> int:
    with _connect_postgres(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    ) as conn:
        with conn.cursor() as cursor:
            _ensure_table(cursor, table_name)
            cursor.execute(_table_sql(table_name, "TRUNCATE TABLE {table_name}"))
            if rows:
                cursor.executemany(
                    _table_sql(
                        table_name,
                        """
                        INSERT INTO {table_name} (
                            entity_name,
                            entity_type,
                            count_3h,
                            count_24h,
                            last_seen_at
                        )
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                    ),
                    [
                        (row.entity_name, row.entity_type, row.count_3h, row.count_24h, row.last_seen_at)
                        for row in rows
                    ],
                )
        conn.commit()

    return len(rows)


def run_ner_dashboard_metrics_job(limit: int = 5000) -> int:
    logging.basicConfig(level=logging.INFO)
    settings = AppSettings.from_env()
    now_utc = datetime.now(timezone.utc)

    client = build_client(
        OpenSearchConfig(
            hosts=settings.opensearch_hosts,
            news_index=settings.opensearch_news_index,
            digests_index=settings.opensearch_digests_index,
            username=settings.opensearch_username,
            password=settings.opensearch_password,
            use_ssl=settings.opensearch_use_ssl,
            verify_certs=settings.opensearch_verify_certs,
        )
    )
    repository = NewsRepository(client, settings.opensearch_news_index)
    items = repository.get_news_for_last_hours(hours=24, now=now_utc, limit=limit)
    rows = _aggregate_entity_metrics(items, now_utc=now_utc)

    written = _refresh_metrics_table(
        rows=rows,
        host=settings.dashboard_pg_host,
        port=settings.dashboard_pg_port,
        database=settings.dashboard_pg_database,
        user=settings.dashboard_pg_user,
        password=settings.dashboard_pg_password,
        table_name=settings.dashboard_pg_table,
    )
    logger.info("Dashboard entity metrics refreshed: %s rows", written)
    return written
