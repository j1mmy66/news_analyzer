from __future__ import annotations

from dataclasses import dataclass

from opensearchpy import OpenSearch


@dataclass(frozen=True)
class OpenSearchConfig:
    hosts: list[str]
    news_index: str
    digests_index: str
    username: str | None = None
    password: str | None = None
    use_ssl: bool = False
    verify_certs: bool = False


def build_client(config: OpenSearchConfig) -> OpenSearch:
    http_auth = None
    if config.username and config.password:
        http_auth = (config.username, config.password)

    return OpenSearch(
        hosts=config.hosts,
        http_auth=http_auth,
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
    )
