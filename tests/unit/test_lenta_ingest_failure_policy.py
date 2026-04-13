from pathlib import Path

import pytest

from news_analyzer.pipeline.ingest import lenta_ingest
from news_analyzer.sources.lenta.collector import LentaCollectStats
from news_analyzer.sources.lenta.config import LentaCollectorConfig


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


def test_run_lenta_ingest_raises_on_fatal_fetch_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Collector:
        def __init__(self, config: LentaCollectorConfig) -> None:
            self.last_stats = LentaCollectStats(fatal_errors=1, fetch_errors=2)

        def collect_latest(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setattr(lenta_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        lenta_ingest.LentaCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: LentaCollectorConfig()),
    )
    monkeypatch.setattr(lenta_ingest, "LentaNewsCollector", _Collector)
    monkeypatch.setattr(lenta_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(lenta_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(lenta_ingest, "NewsRepository", _FakeRepository)

    with pytest.raises(RuntimeError, match="fatal fetch failures"):
        lenta_ingest.run_lenta_ingest()


def test_run_lenta_ingest_degraded_success_when_fatal_but_created(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    class _Collector:
        def __init__(self, config: LentaCollectorConfig) -> None:
            self.last_stats = LentaCollectStats(
                fatal_errors=1,
                fetch_errors=2,
                fetched=3,
                parsed=2,
                full_text_ok=1,
                skipped_no_full_text=1,
            )

        def collect_latest(self) -> list[dict[str, object]]:
            return [{"url": "https://lenta.ru/news/x"}]

    monkeypatch.setattr(lenta_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        lenta_ingest.LentaCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: LentaCollectorConfig()),
    )
    monkeypatch.setattr(lenta_ingest, "LentaNewsCollector", _Collector)
    monkeypatch.setattr(lenta_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(lenta_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(lenta_ingest, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(lenta_ingest, "parse_lenta_article", lambda row: object())
    caplog.set_level("WARNING")

    result = lenta_ingest.run_lenta_ingest()

    assert result == 1
    assert "status=degraded" in caplog.text
    assert "created=1" in caplog.text
    assert "collected_rows=1" in caplog.text
    assert "normalized_rows=1" in caplog.text
    assert "fatal_errors=1" in caplog.text
    assert "fetch_errors=2" in caplog.text
    assert "fetched=3" in caplog.text
    assert "parsed=2" in caplog.text
    assert "full_text_ok=1" in caplog.text
    assert "skipped_no_full_text=1" in caplog.text


def test_run_lenta_ingest_returns_stored_count_without_fatal_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Collector:
        def __init__(self, config: LentaCollectorConfig) -> None:
            self.last_stats = LentaCollectStats()

        def collect_latest(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setattr(lenta_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        lenta_ingest.LentaCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: LentaCollectorConfig()),
    )
    monkeypatch.setattr(lenta_ingest, "LentaNewsCollector", _Collector)
    monkeypatch.setattr(lenta_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(lenta_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(lenta_ingest, "NewsRepository", _FakeRepository)

    assert lenta_ingest.run_lenta_ingest() == 0


def test_run_lenta_ingest_skips_rows_that_fail_to_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Collector:
        def __init__(self, config: LentaCollectorConfig) -> None:
            self.last_stats = LentaCollectStats()

        def collect_latest(self) -> list[dict[str, object]]:
            return [{"url": "https://lenta.ru/news/x"}]

    monkeypatch.setattr(lenta_ingest.AppSettings, "from_env", classmethod(lambda cls: _FakeSettings()))
    monkeypatch.setattr(
        lenta_ingest.LentaCollectorConfig,
        "from_sources_file",
        classmethod(lambda cls, path: LentaCollectorConfig()),
    )
    monkeypatch.setattr(lenta_ingest, "LentaNewsCollector", _Collector)
    monkeypatch.setattr(lenta_ingest, "build_client", lambda config: object())
    monkeypatch.setattr(lenta_ingest, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(lenta_ingest, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(lenta_ingest, "parse_lenta_article", lambda row: (_ for _ in ()).throw(RuntimeError("bad-row")))

    assert lenta_ingest.run_lenta_ingest() == 0
