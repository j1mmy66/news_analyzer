from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from pathlib import Path


@dataclass(frozen=True)
class AppSettings:
    opensearch_hosts: list[str]
    opensearch_news_index: str = "news_items"
    opensearch_digests_index: str = "hourly_digests"
    opensearch_username: str | None = None
    opensearch_password: str | None = None
    opensearch_use_ssl: bool = False
    opensearch_verify_certs: bool = False
    gigachat_base_url: str | None = None
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
        hosts_raw = getenv("OPENSEARCH_HOSTS", "http://opensearch:9200")
        return cls(
            opensearch_hosts=[value.strip() for value in hosts_raw.split(",") if value.strip()],
            opensearch_news_index=getenv("OPENSEARCH_NEWS_INDEX", "news_items"),
            opensearch_digests_index=getenv("OPENSEARCH_DIGESTS_INDEX", "hourly_digests"),
            opensearch_username=getenv("OPENSEARCH_USERNAME"),
            opensearch_password=getenv("OPENSEARCH_PASSWORD"),
            opensearch_use_ssl=getenv("OPENSEARCH_USE_SSL", "false").lower() == "true",
            opensearch_verify_certs=getenv("OPENSEARCH_VERIFY_CERTS", "false").lower() == "true",
            gigachat_base_url=getenv("GIGACHAT_BASE_URL"),
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
