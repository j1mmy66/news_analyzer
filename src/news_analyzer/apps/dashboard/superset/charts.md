# Superset Charts

Dashboard: `NER Entities Overview`

- `Top 10 Entities (3h)` horizontal bar chart:
  - Dataset: `ner_entity_metrics`
  - Metric: `SUM(count_3h)`
  - Dimension: `entity_name`
  - Limit: `10`
  - Order: metric descending

- `Top 10 Entities (24h)` horizontal bar chart:
  - Dataset: `ner_entity_metrics`
  - Metric: `SUM(count_24h)`
  - Dimension: `entity_name`
  - Limit: `10`
  - Order: metric descending

- `Top 100 Entities Table`:
  - Dataset: `ner_entity_metrics`
  - Columns: `entity_name`, `entity_type`, `count_3h`, `count_24h`, `last_seen_at`
  - Limit: `100`
  - Sort: `count_24h DESC`
