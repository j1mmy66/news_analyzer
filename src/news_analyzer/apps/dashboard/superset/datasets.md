# Superset Datasets

Use a Postgres-backed dashboard dataset:

- `ner_entity_metrics` (`public.ner_entity_metrics`) as the main dataset for NER exploration.

`ner_entity_metrics` fields:

- `entity_name` (`text`) entity key, built from `entities.normalized` with fallback to `entities.text`.
- `entity_type` (`text`) entity label from NER output.
- `count_3h` (`int`) mention count for the last 3 hours in UTC.
- `count_24h` (`int`) mention count for the last 24 hours in UTC.
- `last_seen_at` (`timestamptz`) last UTC publication time where the entity appeared.

Excluded entities for dashboard aggregation:

- `РБК`
- `MAX`
- `MAХ`
- `MAX!`
- `MAХ!`

This table is refreshed by Airflow DAG `dashboard_ner_metrics` every 15 minutes.
