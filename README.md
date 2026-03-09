# News Analyzer

## Быстрый запуск

1. Подготовьте `src/news_analyzer/settings/sources.yaml`.

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

2. Добавьте переменные окружения для Airflow сервисов в `docker-compose.yml` (в секции `environment` у `airflow-webserver` и `airflow-scheduler`):

- `SOURCES_CONFIG_PATH=src/news_analyzer/settings/sources.yaml`
- `OPENSEARCH_HOSTS=http://opensearch:9200`
- `GIGACHAT_BASE_URL=<gigachat_endpoint>`
- `GIGACHAT_API_KEY=<gigachat_api_key>`
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

3. Подготовьте локальные артефакты NER-моделей в директории `models/`:

- `models/slovnet_ner_news_v1.tar`
- `models/navec_news_v1_1B_250K_300d_100q.tar`
- `models/any-news-classifier/` (локальный snapshot модели `data-silence/any-news-classifier`, без онлайн-загрузки в runtime)

4. Поднимите инфраструктуру:

```bash
docker compose up airflow-init
docker compose up -d opensearch opensearch-dashboards postgres airflow-webserver airflow-scheduler superset
```

5. Откройте Airflow: `http://localhost:8080`.

- Логин: `admin`
- Пароль: `admin`

OpenSearch Dashboards: `http://localhost:5601`.

Superset: `http://localhost:8088` (admin/admin).

6. Включите DAG-и и запустите их:

- `rbc_news_ingest`
- `news_nlp_enrichment`
- `news_summaries`
- `news_retry_missing_summaries`
- `dashboard_ner_metrics`

7. Импортируйте assets в Superset:

```bash
docker compose exec -T superset superset import-directory -o /app/superset/assets
```

Импорт добавит:

- database `news_analyzer_postgres`
- dataset `public.ner_entity_metrics`
- dashboard `NER Entities Overview`

## Запуск Streamlit

1. Установите зависимости локально:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Укажите OpenSearch для локального процесса:

```bash
export OPENSEARCH_HOSTS=http://localhost:9200
export OPENSEARCH_NEWS_INDEX=news_items
export OPENSEARCH_DIGESTS_INDEX=hourly_digests
```

3. Запустите приложение:

```bash
PYTHONPATH=src streamlit run src/news_analyzer/apps/streamlit/app.py
```

## Проверка

1. Убедитесь, что в индексе `news_items` появляются документы из RBC.
2. Убедитесь, что поля `class_label`, `entities`, `summary` заполняются после выполнения enrichment/summaries DAG-ов.
3. Убедитесь, что в индексе `hourly_digests` появляются hourly digest документы.
4. Откройте OpenSearch Dashboards (`http://localhost:5601`) и проверьте индексы:
   - `news_items`
   - `hourly_digests`
5. В Dashboards откройте Discover и убедитесь, что документы из `news_items` отображаются.
6. Проверьте в Postgres, что обновляется таблица `ner_entity_metrics` (после запуска `dashboard_ner_metrics`).
7. Откройте Superset (`http://localhost:8088`) и проверьте dashboard `NER Entities Overview`:
   - `Top 10 Entities (3h)`
   - `Top 10 Entities (24h)`
   - `Top 100 Entities Table`

## Локальные тесты

```bash
python3 -m pytest -q
```

Если `pytest` не найден:

```bash
pip install -r requirements.txt
```
