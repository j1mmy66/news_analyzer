# News Analyzer

`News Analyzer` - система потокового анализа новостей на русском языке.

## Что делает проект

- собирает новости с страниц RBC и нормализует данные
- сохраняет raw/normalized контент и метаданные
- извлекает именованные сущности (NER) в структурированном виде
- классифицирует каждую новость (`class_label`, `class_confidence`)
- генерирует per-item summary и hourly digest через Sber GigaChat API
- предоставляет два UX-слоя:
  - Streamlit-приложение для ленты новостей и summary
  - аналитический dashboard (Superset) для entity-centric анализа

## Архитектура

- `sources/rbc` - сбор и парсинг новостей RBC
- `pipeline/ingest` - загрузка в хранилище
- `pipeline/enrich` - NER + классификация
- `pipeline/summarize` - per-item summary, hourly digest, retry пропущенных summary
- `pipeline/dashboard` - агрегация метрик сущностей для Superset
- `storage/opensearch` - индексы и репозитории для новостей и digest
- `apps/streamlit` - пользовательский интерфейс ленты и digest
- `apps/dashboard/superset` - assets для NER dashboard
- `dags/` - orchestration в Airflow


## Технологии

- Python
- Apache Airflow
- OpenSearch
- Streamlit
- Apache Superset
- Sber GigaChat API


