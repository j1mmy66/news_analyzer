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
            sources_config_path=Path(getenv("SOURCES_CONFIG_PATH", "src/news_analyzer/settings/sources.yaml")),
        )
