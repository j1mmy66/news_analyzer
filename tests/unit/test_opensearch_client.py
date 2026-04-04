from __future__ import annotations

from news_analyzer.storage.opensearch import client as os_client
from news_analyzer.storage.opensearch.client import OpenSearchConfig


def test_build_client_with_http_auth(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_open_search(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(os_client, "OpenSearch", _fake_open_search)

    config = OpenSearchConfig(
        hosts=["http://localhost:9200"],
        news_index="news",
        digests_index="digests",
        username="user",
        password="pass",
        use_ssl=True,
        verify_certs=True,
    )

    os_client.build_client(config)

    assert captured["hosts"] == ["http://localhost:9200"]
    assert captured["http_auth"] == ("user", "pass")
    assert captured["use_ssl"] is True
    assert captured["verify_certs"] is True


def test_build_client_without_http_auth(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_open_search(**kwargs):
        captured.update(kwargs)
        return object()

    monkeypatch.setattr(os_client, "OpenSearch", _fake_open_search)

    config = OpenSearchConfig(
        hosts=["http://localhost:9200"],
        news_index="news",
        digests_index="digests",
        username=None,
        password=None,
        use_ssl=False,
        verify_certs=False,
    )

    os_client.build_client(config)

    assert captured["http_auth"] is None
    assert captured["use_ssl"] is False
    assert captured["verify_certs"] is False
