from pathlib import Path

import pytest

from news_analyzer.pipeline.ingest import rbc_ingest
from news_analyzer.sources.rbc.collector import RBCCollectStats
from news_analyzer.sources.rbc.config import RBCCollectorConfig


class _FakeSettings:
    opensearch_hosts = ["http://opensearch:9200"]
    opensearch_news_index = "news_items"
    opensearch_digests_index = "hourly_digests"
    opensearch_username = None
    opensearch_password = None
    opensearch_use_ssl = False
    opensearch_verify_certs = False
    sources_config_path = Path("dummy.yaml")


class _FakeIndexManager:
    def __init__(self, client: object) -> None:
        self._client = client

    def ensure(self, index_name: str, mapping_file: str) -> None:
        return None


class _FakeRepository:
    def __init__(self, client: object, index_name: str) -> None:
        self._client = client
        self._index_name = index_name

    def upsert_news(self, items) -> int:
        return len(list(items))


def test_run_rbc_ingest_raises_on_fatal_fetch_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Collector:
        def __init__(self, config: RBCCollectorConfig) -> None:
            self.last_stats = RBCCollectStats(
                fatal_errors=1,
                fetch_errors_total=2,
                failed_sections=["economics"],
            )

        def collect_latest(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setattr(rbc_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        rbc_ingest.RBCCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: RBCCollectorConfig(sections=["economics"])),
    )
    monkeypatch.setattr(rbc_ingest, "RBCNewsCollector", _Collector)
    monkeypatch.setattr(rbc_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(rbc_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(rbc_ingest, "NewsRepository", _FakeRepository)

    with pytest.raises(RuntimeError, match="fatal fetch failures"):
        rbc_ingest.run_rbc_ingest()


def test_run_rbc_ingest_degraded_success_when_fatal_but_created(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    class _Collector:
        def __init__(self, config: RBCCollectorConfig) -> None:
            self.last_stats = RBCCollectStats(
                fatal_errors=1,
                fetch_errors_total=2,
                failed_sections=["economics"],
            )

        def collect_latest(self) -> list[dict[str, object]]:
            return [{"url": "https://www.rbc.ru/x"}]

    monkeypatch.setattr(rbc_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        rbc_ingest.RBCCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: RBCCollectorConfig(sections=["economics"])),
    )
    monkeypatch.setattr(rbc_ingest, "RBCNewsCollector", _Collector)
    monkeypatch.setattr(rbc_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(rbc_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(rbc_ingest, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(rbc_ingest, "parse_rbc_article", lambda row: object())
    caplog.set_level("WARNING")

    result = rbc_ingest.run_rbc_ingest()

    assert result == 1
    assert "status=degraded" in caplog.text
    assert "created=1" in caplog.text
    assert "collected_rows=1" in caplog.text
    assert "normalized_rows=1" in caplog.text
    assert "fatal_errors=1" in caplog.text
    assert "fetch_errors_total=2" in caplog.text


def test_run_rbc_ingest_returns_stored_count_without_fatal_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Collector:
        def __init__(self, config: RBCCollectorConfig) -> None:
            self.last_stats = RBCCollectStats()

        def collect_latest(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setattr(rbc_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        rbc_ingest.RBCCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: RBCCollectorConfig(sections=["economics"])),
    )
    monkeypatch.setattr(rbc_ingest, "RBCNewsCollector", _Collector)
    monkeypatch.setattr(rbc_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(rbc_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(rbc_ingest, "NewsRepository", _FakeRepository)

    assert rbc_ingest.run_rbc_ingest() == 0


def test_run_rbc_ingest_skips_rows_that_fail_to_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Collector:
        def __init__(self, config: RBCCollectorConfig) -> None:
            self.last_stats = RBCCollectStats()

        def collect_latest(self) -> list[dict[str, object]]:
            return [{"url": "https://www.rbc.ru/x"}]

    monkeypatch.setattr(rbc_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        rbc_ingest.RBCCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: RBCCollectorConfig(sections=["economics"])),
    )
    monkeypatch.setattr(rbc_ingest, "RBCNewsCollector", _Collector)
    monkeypatch.setattr(rbc_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(rbc_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(rbc_ingest, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(rbc_ingest, "parse_rbc_article", lambda row: (_ for _ in ()).throw(RuntimeError("bad-row")))

    assert rbc_ingest.run_rbc_ingest() == 0
