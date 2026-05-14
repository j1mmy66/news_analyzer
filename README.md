# News Analyzer

`News Analyzer` - система потокового анализа русскоязычных новостей с отдельными интерфейсами для новостной ленты и NER-аналитики.

## Актуальный функционал

- извлечение новостных метериалов из `RBC` и `Lenta`;
- семантическая дедубликация материалов
- выделение именованных сущностей (NER)
- тематическая классификация
- суммаризация каждой новости и формирование часового дайджеста
- Streamlit UI для чтения ленты и суммаризаций;
- Superset dashboard для NER аналитики.

## Пайплайн `news_unified_pipeline`

DAG запускается каждые 30 минут (`*/30 * * * *`) и выполняет этапы:

1. `rbc_ingest`
2. `lenta_ingest`
3. `ingest_gate` (пайплайн продолжается, если успешно отработал хотя бы один источник)
4. `semantic_dedup`
5. `ner_and_classification`
6. `item_summaries`
7. `hourly_digest`
8. `refresh_ner_entity_metrics`

## Актуальная структура проекта

- `dags/news_unified_pipeline_dag.py` - orchestration в Airflow.
- `src/news_analyzer/sources/` - коллекторы и парсеры источников (`rbc`, `lenta`).
- `src/news_analyzer/pipeline/ingest/` - извлечение + политики деградации.
- `src/news_analyzer/pipeline/dedup/` - семантическая дедупликация.
- `src/news_analyzer/pipeline/enrich/` - NER + классификация.
- `src/news_analyzer/pipeline/summarize/` - суммаризация.
- `src/news_analyzer/pipeline/dashboard/` - расчёт `ner_entity_metrics` для Superset.
- `src/news_analyzer/storage/opensearch/` - взаимодействие с хранилищем новостей OpenSearch 
- `src/news_analyzer/apps/streamlit/` - пользовательское приложение.
- `src/news_analyzer/apps/dashboard/superset/` - Superset.
- `src/news_analyzer/settings/` - настройки времени выполнения и конфигурация источников.
- `tests/` - тесты.

## Технологии

- Python 3.11+
- Apache Airflow
- OpenSearch
- Streamlit
- Apache Superset
- PostgreSQL
- Sber GigaChat API

