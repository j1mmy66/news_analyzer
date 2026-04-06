from __future__ import annotations

from datetime import datetime, timezone

from news_analyzer.domain.enums import ProcessingStatus
from news_analyzer.domain.models import SummaryResult
from news_analyzer.pipeline.summarize import hourly_digest_job, item_summary_job


class _SettingsNoCredentials:
    opensearch_hosts = ["http://opensearch:9200"]
    opensearch_news_index = "news_items"
    opensearch_digests_index = "hourly_digests"
    opensearch_username = None
    opensearch_password = None
    opensearch_use_ssl = False
    opensearch_verify_certs = False
    gigachat_auth_key = None
    gigachat_api_key = None
    gigachat_scope = "GIGACHAT_API_PERS"
    gigachat_model = "GigaChat"
    gigachat_timeout_seconds = 15.0
    gigachat_max_retries = 3
    gigachat_verify_ssl = True


class _SettingsLegacyCredential(_SettingsNoCredentials):
    gigachat_api_key = "legacy-key"


class _SettingsWithCredential(_SettingsNoCredentials):
    gigachat_auth_key = "new-key"


def test_item_summary_job_skips_when_credentials_missing(monkeypatch) -> None:
    monkeypatch.setattr(item_summary_job.AppSettings, "from_env", classmethod(lambda cls: _SettingsNoCredentials()))
    assert item_summary_job.run_item_summary_job(limit=10) == 0


def test_item_summary_job_uses_legacy_key_fallback(monkeypatch) -> None:
    captured = {"auth_key": None}

    class _FakeGigaChatClient:
        def __init__(self, **kwargs) -> None:
            captured["auth_key"] = kwargs.get("auth_key")

    class _FakeRepository:
        saved: list[SummaryResult] = []

        def __init__(self, client: object, index_name: str) -> None:
            return None

        def get_recent_news_without_summary(self, limit: int = 100):
            return [{"external_id": "id-1", "cleaned_text": "text"}]

        def set_summary(self, external_id: str, summary: SummaryResult) -> None:
            self.saved.append(summary)

    class _FakeSummaryService:
        def __init__(self, client: object) -> None:
            return None

        def summarize_item(self, text: str) -> SummaryResult:
            return SummaryResult(
                summary="ok",
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )

    monkeypatch.setattr(item_summary_job.AppSettings, "from_env", classmethod(lambda cls: _SettingsLegacyCredential()))
    monkeypatch.setattr(item_summary_job, "build_client", lambda config: object())
    monkeypatch.setattr(item_summary_job, "NewsRepository", _FakeRepository)
    monkeypatch.setattr(item_summary_job, "SummaryService", _FakeSummaryService)
    monkeypatch.setattr(item_summary_job, "GigaChatClient", _FakeGigaChatClient)

    assert item_summary_job.run_item_summary_job(limit=10) == 1
    assert captured["auth_key"] == "legacy-key"


def test_hourly_digest_job_does_not_create_digest_when_summary_failed(monkeypatch) -> None:
    calls = {"digest_upserted": False, "digest_linked": False}

    class _FakeIndexManager:
        def __init__(self, client: object) -> None:
            return None

        def ensure(self, index_name: str, mapping_file: str) -> None:
            return None

    class _FakeNewsRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def get_news_for_last_hour(self):
            return [{"external_id": "id-1", "cleaned_text": "text"}]

        def set_hourly_digest_link(self, external_ids: list[str], digest_id: str) -> None:
            calls["digest_linked"] = True

    class _FakeDigestRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def upsert(self, digest: object) -> None:
            calls["digest_upserted"] = True

    class _FakeSummaryService:
        def __init__(self, client: object) -> None:
            return None

        def summarize_hour(self, texts: list[str]) -> SummaryResult:
            return SummaryResult(
                summary=None,
                status=ProcessingStatus.FAILED,
                error_code="GigaChatServerError",
                updated_at=datetime.now(timezone.utc),
            )

    monkeypatch.setattr(hourly_digest_job.AppSettings, "from_env", classmethod(lambda cls: _SettingsWithCredential()))
    monkeypatch.setattr(hourly_digest_job, "build_client", lambda config: object())
    monkeypatch.setattr(hourly_digest_job, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(hourly_digest_job, "NewsRepository", _FakeNewsRepository)
    monkeypatch.setattr(hourly_digest_job, "HourlyDigestRepository", _FakeDigestRepository)
    monkeypatch.setattr(hourly_digest_job, "SummaryService", _FakeSummaryService)
    monkeypatch.setattr(hourly_digest_job, "GigaChatClient", lambda **kwargs: object())

    result = hourly_digest_job.run_hourly_digest_job()

    assert result is None
    assert calls["digest_upserted"] is False
    assert calls["digest_linked"] is False


def test_hourly_digest_job_skips_when_credentials_missing(monkeypatch) -> None:
    monkeypatch.setattr(hourly_digest_job.AppSettings, "from_env", classmethod(lambda cls: _SettingsNoCredentials()))
    assert hourly_digest_job.run_hourly_digest_job() is None


def test_hourly_digest_job_uses_legacy_key_and_skips_when_no_texts(monkeypatch, caplog) -> None:
    class _FakeIndexManager:
        def __init__(self, client: object) -> None:
            return None

        def ensure(self, index_name: str, mapping_file: str) -> None:
            return None

    class _FakeNewsRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def get_news_for_last_hour(self):
            return [{"external_id": "id-1", "cleaned_text": ""}]

        def set_hourly_digest_link(self, external_ids: list[str], digest_id: str) -> None:
            raise AssertionError("Should not link digest when texts are empty")

    class _FakeDigestRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def upsert(self, digest: object) -> None:
            raise AssertionError("Should not upsert digest when texts are empty")

    class _FakeSummaryService:
        def __init__(self, client: object) -> None:
            raise AssertionError("SummaryService should not be created without texts")

    class _FakeGigaChatClient:
        def __init__(self, **kwargs) -> None:
            raise AssertionError("GigaChatClient should not be created without texts")

    monkeypatch.setattr(hourly_digest_job.AppSettings, "from_env", classmethod(lambda cls: _SettingsLegacyCredential()))
    monkeypatch.setattr(hourly_digest_job, "build_client", lambda config: object())
    monkeypatch.setattr(hourly_digest_job, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(hourly_digest_job, "NewsRepository", _FakeNewsRepository)
    monkeypatch.setattr(hourly_digest_job, "HourlyDigestRepository", _FakeDigestRepository)
    monkeypatch.setattr(hourly_digest_job, "SummaryService", _FakeSummaryService)
    monkeypatch.setattr(hourly_digest_job, "GigaChatClient", _FakeGigaChatClient)

    assert hourly_digest_job.run_hourly_digest_job() is None
    assert "deprecated" in caplog.text


def test_hourly_digest_job_successfully_creates_digest_and_links_news(monkeypatch) -> None:
    calls: dict[str, object] = {"linked": None, "digest": None}

    class _FakeDatetime:
        @staticmethod
        def now(tz):
            assert tz == timezone.utc
            return datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)

    class _FakeIndexManager:
        def __init__(self, client: object) -> None:
            return None

        def ensure(self, index_name: str, mapping_file: str) -> None:
            return None

    class _FakeNewsRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def get_news_for_last_hour(self):
            return [
                {"external_id": "id-1", "cleaned_text": "text-1"},
                {"external_id": "id-2", "cleaned_text": "text-2"},
            ]

        def set_hourly_digest_link(self, external_ids: list[str], digest_id: str) -> None:
            calls["linked"] = (external_ids, digest_id)

    class _FakeDigestRepository:
        def __init__(self, client: object, index_name: str) -> None:
            return None

        def upsert(self, digest: object) -> None:
            calls["digest"] = digest

    class _FakeSummaryService:
        def __init__(self, client: object) -> None:
            return None

        def summarize_hour(self, texts: list[str]) -> SummaryResult:
            assert texts == ["text-1", "text-2"]
            return SummaryResult(
                summary="hour summary",
                status=ProcessingStatus.SUCCESS,
                error_code=None,
                updated_at=datetime.now(timezone.utc),
            )

    monkeypatch.setattr(hourly_digest_job, "datetime", _FakeDatetime)
    monkeypatch.setattr(hourly_digest_job.AppSettings, "from_env", classmethod(lambda cls: _SettingsWithCredential()))
    monkeypatch.setattr(hourly_digest_job, "build_client", lambda config: object())
    monkeypatch.setattr(hourly_digest_job, "OpenSearchIndexManager", _FakeIndexManager)
    monkeypatch.setattr(hourly_digest_job, "NewsRepository", _FakeNewsRepository)
    monkeypatch.setattr(hourly_digest_job, "HourlyDigestRepository", _FakeDigestRepository)
    monkeypatch.setattr(hourly_digest_job, "SummaryService", _FakeSummaryService)
    monkeypatch.setattr(hourly_digest_job, "GigaChatClient", lambda **kwargs: object())

    digest_id = hourly_digest_job.run_hourly_digest_job()

    assert digest_id == "digest-2026031708"
    assert calls["linked"] == (["id-1", "id-2"], "digest-2026031708")
    assert calls["digest"] is not None
