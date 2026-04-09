from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from pathlib import Path


def _default_opensearch_hosts() -> str:
    explicit = getenv("OPENSEARCH_HOSTS")
    if explicit:
        return explicit
    # In docker-compose network service name is resolvable as "opensearch".
    if Path("/.dockerenv").exists():
        return "http://opensearch:9200"
    # For local host runs fallback to mapped docker port.
    return "http://localhost:9200"


@dataclass(frozen=True)
class AppSettings:
    opensearch_hosts: list[str]
    opensearch_news_index: str = "news_items"
    opensearch_digests_index: str = "hourly_digests"
    opensearch_username: str | None = None
    opensearch_password: str | None = None
    opensearch_use_ssl: bool = False
    opensearch_verify_certs: bool = False
    dashboard_pg_host: str = "postgres"
    dashboard_pg_port: int = 5432
    dashboard_pg_database: str = "airflow"
    dashboard_pg_user: str = "airflow"
    dashboard_pg_password: str = "airflow"
    dashboard_pg_table: str = "ner_entity_metrics"
    gigachat_auth_key: str | None = None
    gigachat_scope: str = "GIGACHAT_API_PERS"
    gigachat_model: str = "GigaChat"
    gigachat_verify_ssl: bool = True
    gigachat_api_key: str | None = None
    gigachat_timeout_seconds: float = 15.0
    gigachat_max_retries: int = 3
    classifier_model_path: Path = Path("models/any-news-classifier")
    classifier_device: str = "cpu"
    classifier_max_length: int = 512
    ner_slovnet_model_path: Path = Path("models/slovnet_ner_news_v1.tar")
    ner_navec_path: Path = Path("models/navec_news_v1_1B_250K_300d_100q.tar")
    ner_max_retries: int = 2
    ner_retry_backoff_seconds: float = 0.5
    ner_retry_backoff_cap_seconds: float = 5.0
    sources_config_path: Path = Path("src/news_analyzer/settings/sources.yaml")

    @classmethod
    def from_env(cls) -> "AppSettings":
        hosts_raw = _default_opensearch_hosts()
        return cls(
            opensearch_hosts=[value.strip() for value in hosts_raw.split(",") if value.strip()],
            opensearch_news_index=getenv("OPENSEARCH_NEWS_INDEX", "news_items"),
            opensearch_digests_index=getenv("OPENSEARCH_DIGESTS_INDEX", "hourly_digests"),
            opensearch_username=getenv("OPENSEARCH_USERNAME"),
            opensearch_password=getenv("OPENSEARCH_PASSWORD"),
            opensearch_use_ssl=getenv("OPENSEARCH_USE_SSL", "false").lower() == "true",
            opensearch_verify_certs=getenv("OPENSEARCH_VERIFY_CERTS", "false").lower() == "true",
            dashboard_pg_host=getenv("DASHBOARD_PG_HOST", "postgres"),
            dashboard_pg_port=int(getenv("DASHBOARD_PG_PORT", "5432")),
            dashboard_pg_database=getenv("DASHBOARD_PG_DATABASE", "airflow"),
            dashboard_pg_user=getenv("DASHBOARD_PG_USER", "airflow"),
            dashboard_pg_password=getenv("DASHBOARD_PG_PASSWORD", "airflow"),
            dashboard_pg_table=getenv("DASHBOARD_PG_TABLE", "ner_entity_metrics"),
            gigachat_auth_key=getenv("GIGACHAT_AUTH_KEY"),
            gigachat_scope=getenv("GIGACHAT_SCOPE", "GIGACHAT_API_PERS"),
            gigachat_model=getenv("GIGACHAT_MODEL", "GigaChat"),
            gigachat_verify_ssl=getenv("GIGACHAT_VERIFY_SSL", "true").lower() == "true",
            gigachat_api_key=getenv("GIGACHAT_API_KEY"),
            gigachat_timeout_seconds=float(getenv("GIGACHAT_TIMEOUT_SECONDS", "15")),
            gigachat_max_retries=int(getenv("GIGACHAT_MAX_RETRIES", "3")),
            classifier_model_path=Path(getenv("CLASSIFIER_MODEL_PATH", "models/any-news-classifier")),
            classifier_device=getenv("CLASSIFIER_DEVICE", "cpu"),
            classifier_max_length=int(getenv("CLASSIFIER_MAX_LENGTH", "512")),
            ner_slovnet_model_path=Path(getenv("NER_SLOVNET_MODEL_PATH", "models/slovnet_ner_news_v1.tar")),
            ner_navec_path=Path(getenv("NER_NAVEC_PATH", "models/navec_news_v1_1B_250K_300d_100q.tar")),
            ner_max_retries=int(getenv("NER_MAX_RETRIES", "2")),
            ner_retry_backoff_seconds=float(getenv("NER_RETRY_BACKOFF_SECONDS", "0.5")),
            ner_retry_backoff_cap_seconds=float(getenv("NER_RETRY_BACKOFF_CAP_SECONDS", "5")),
            sources_config_path=Path(getenv("SOURCES_CONFIG_PATH", "src/news_analyzer/settings/sources.yaml")),
        )
