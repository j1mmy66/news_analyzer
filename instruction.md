# Инструкция По Запуску

Этот документ содержит практические шаги по локальному запуску контура ingestion + NLP + summaries + dashboards.

## 1. Предварительные требования

- Docker и Docker Compose
- Python 3.11+ (для локального запуска Streamlit и тестов)
- Локальные артефакты моделей NER/классификации (см. ниже)

## 2. Конфигурация источников

Подготовьте `src/news_analyzer/settings/sources.yaml`:

```yaml
rbc:
  sections:
    - economics
    - society
  request_timeout: 20
  pages_limit: 2
  max_retries: 3
  backoff_seconds: 0.5
  fallback_enabled: true
  user_agent: "news-analyzer-rbc-collector/1.0"
```

## 3. Переменные окружения Airflow

Убедитесь, что в `docker-compose.yml` для `airflow-webserver` и `airflow-scheduler` заданы:

- `SOURCES_CONFIG_PATH=src/news_analyzer/settings/sources.yaml`
- `OPENSEARCH_HOSTS=http://opensearch:9200`
- `GIGACHAT_AUTH_KEY=<gigachat_authorization_key>`
- `GIGACHAT_SCOPE=GIGACHAT_API_PERS`
- `GIGACHAT_MODEL=GigaChat`
- `GIGACHAT_TIMEOUT_SECONDS=15`
- `GIGACHAT_MAX_RETRIES=3`
- `GIGACHAT_VERIFY_SSL=true`
- `GIGACHAT_API_KEY=<legacy_optional_fallback>`
- `NER_SLOVNET_MODEL_PATH=/opt/airflow/app/models/slovnet_ner_news_v1.tar`
- `NER_NAVEC_PATH=/opt/airflow/app/models/navec_news_v1_1B_250K_300d_100q.tar`
- `NER_MAX_RETRIES=2`
- `NER_RETRY_BACKOFF_SECONDS=0.5`
- `NER_RETRY_BACKOFF_CAP_SECONDS=5`
- `CLASSIFIER_MODEL_PATH=/opt/airflow/app/models/any-news-classifier`
- `CLASSIFIER_DEVICE=cpu`
- `CLASSIFIER_MAX_LENGTH=512`
- `DASHBOARD_PG_HOST=postgres`
- `DASHBOARD_PG_PORT=5432`
- `DASHBOARD_PG_DATABASE=airflow`
- `DASHBOARD_PG_USER=airflow`
- `DASHBOARD_PG_PASSWORD=airflow`
- `DASHBOARD_PG_TABLE=ner_entity_metrics`

## 4. Модели

Подготовьте локально директорию `models/`:

- `models/slovnet_ner_news_v1.tar`
- `models/navec_news_v1_1B_250K_300d_100q.tar`
- `models/any-news-classifier/` (snapshot `data-silence/any-news-classifier`)

## 5. Подъём инфраструктуры

```bash
docker compose up airflow-init
docker compose up -d opensearch opensearch-dashboards postgres airflow-webserver airflow-scheduler superset
```

UI:

- Airflow: `http://localhost:8080` (`admin/admin`)
- OpenSearch Dashboards: `http://localhost:5601`
- Superset: `http://localhost:8088` (`admin/admin`)

## 6. Запуск DAG-ов

В Airflow включите и запустите:

- `rbc_news_ingest`
- `news_nlp_enrichment`
- `news_summaries`
- `news_retry_missing_summaries`
- `dashboard_ner_metrics`

## 7. Импорт Superset assets

```bash
docker compose exec -T superset superset import-directory -o /app/superset/assets
```

После импорта появятся:

- `news_analyzer_postgres` (database)
- `public.ner_entity_metrics` (dataset)
- `NER Entities Overview` (dashboard)

## 8. Локальный запуск Streamlit

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

```bash
export OPENSEARCH_HOSTS=http://localhost:9200
export OPENSEARCH_NEWS_INDEX=news_items
export OPENSEARCH_DIGESTS_INDEX=hourly_digests
```

```bash
PYTHONPATH=src streamlit run src/news_analyzer/apps/streamlit/app.py
```

## 9. Проверка после запуска

1. В `news_items` появляются новости RBC.
2. Поля `class_label`, `entities`, `summary` заполняются после enrichment/summaries DAG-ов.
3. В `hourly_digests` появляются hourly digest записи.
4. В OpenSearch Dashboards видны индексы `news_items` и `hourly_digests`.
5. В Superset обновляется dashboard `NER Entities Overview`.
6. В Postgres обновляется `ner_entity_metrics` (после `dashboard_ner_metrics`).

## 10. Тесты

```bash
python3 -m pytest -q
```

Если `pytest` не найден:

```bash
pip install -r requirements.txt
```
