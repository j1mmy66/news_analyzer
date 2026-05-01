# Описание запуска программы

Полный контур разворачивается через Docker Compose. Локальные NLP-модели в
репозиторий не входят, поэтому после клонирования их необходимо скачать отдельно в каталог
models/.

## Предварительные требования

Перед развертыванием должны быть установлены:
- Git;
- Docker и Docker Compose;
- Python 3.11+;
- доступ к сети интернет для загрузки Python-зависимостей и моделей;
- ключ доступа к Sber GigaChat, если требуется генерация суммаризаций.

GPU для запуска не требуется. Базовый сценарий поддерживает работу на CPU.

## Развертывание

Команды подготовки выполняются один раз после клонирования репозитория; дальнейшие
запуски выполняются скриптом `./scripts/start_project.sh`.

```
git clone https://github.com/j1mmy66/news_analyzer.git
cd news_analyzer

cat > .env <<'EOF'
AIRFLOW__WEBSERVER__SECRET_KEY=change-me-please
GIGACHAT_AUTH_KEY=
GIGACHAT_SCOPE=GIGACHAT_API_PERS
GIGACHAT_MODEL=GigaChat
GIGACHAT_TIMEOUT_SECONDS=15
GIGACHAT_MAX_RETRIES=3
GIGACHAT_VERIFY_SSL=true
EOF

mkdir -p models

curl -L https://storage.yandexcloud.net/natasha-
slovnet/packs/slovnet_ner_news_v1.tar \
-o models/slovnet_ner_news_v1.tar

curl -L https://storage.yandexcloud.net/natasha-
navec/packs/navec_news_v1_1B_250K_300d_100q.tar \
-o models/navec_news_v1_1B_250K_300d_100q.tar

python3 -m pip install "huggingface_hub[cli]"

hf download data-silence/rus-news-classifier \
--local-dir models/any-news-classifier

hf download sentence-transformers/paraphrase-multilingual-MiniLM-
L12-v2 \
--local-dir models/dedup-paraphrase-multilingual-MiniLM-L12-v2

./scripts/start_project.sh
```
Если требуется суммаризаций, в .env необходимо указать значение GIGACHAT_AUTH_KEY
до запуска скрипта.
После успешного завершения `./scripts/start_project.sh` скрипт выведет адреса сервисов:
- Airflow: http://localhost:8080 (admin / admin);
- Streamlit: http://localhost:8501;
- OpenSearch: http://localhost:9200;
- Superset: http://localhost:8088 (admin / admin);

OpenSearch Dashboards: http://localhost:5601.
Далее необходимо выполнить действия:
- 1 Открыть Airflow по адресу http://localhost:8080.
- 2 Включить и запустить DAG news_unified_pipeline.