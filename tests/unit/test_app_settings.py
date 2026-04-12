from __future__ import annotations

from news_analyzer.settings.app_settings import AppSettings, _default_opensearch_hosts


def test_app_settings_reads_gigachat_env(monkeypatch) -> None:
    monkeypatch.setenv("OPENSEARCH_HOSTS", "http://opensearch:9200")
    monkeypatch.setenv("GIGACHAT_AUTH_KEY", "auth-key")
    monkeypatch.setenv("GIGACHAT_SCOPE", "GIGACHAT_API_CORP")
    monkeypatch.setenv("GIGACHAT_MODEL", "GigaChat")
    monkeypatch.setenv("GIGACHAT_VERIFY_SSL", "false")

    settings = AppSettings.from_env()

    assert settings.gigachat_auth_key == "auth-key"
    assert settings.gigachat_scope == "GIGACHAT_API_CORP"
    assert settings.gigachat_model == "GigaChat"
    assert settings.gigachat_verify_ssl is False


def test_app_settings_gigachat_defaults(monkeypatch) -> None:
    monkeypatch.setenv("OPENSEARCH_HOSTS", "http://opensearch:9200")
    monkeypatch.delenv("GIGACHAT_AUTH_KEY", raising=False)
    monkeypatch.delenv("GIGACHAT_SCOPE", raising=False)
    monkeypatch.delenv("GIGACHAT_MODEL", raising=False)
    monkeypatch.delenv("GIGACHAT_VERIFY_SSL", raising=False)

    settings = AppSettings.from_env()

    assert settings.gigachat_auth_key is None
    assert settings.gigachat_scope == "GIGACHAT_API_PERS"
    assert settings.gigachat_model == "GigaChat"
    assert settings.gigachat_verify_ssl is True


def test_app_settings_reads_dedup_env(monkeypatch) -> None:
    monkeypatch.setenv("OPENSEARCH_HOSTS", "http://opensearch:9200")
    monkeypatch.setenv("DEDUP_MODEL_NAME", "custom-model")
    monkeypatch.setenv("DEDUP_SIMILARITY_THRESHOLD", "0.87")
    monkeypatch.setenv("DEDUP_WINDOW_HOURS", "5")
    monkeypatch.setenv("DEDUP_LOOKBACK_HOURS", "48")
    monkeypatch.setenv("DEDUP_TEXT_CHARS", "1234")
    monkeypatch.setenv("DEDUP_DEVICE", "cpu")

    settings = AppSettings.from_env()

    assert settings.dedup_model_name == "custom-model"
    assert settings.dedup_similarity_threshold == 0.87
    assert settings.dedup_window_hours == 5
    assert settings.dedup_lookback_hours == 48
    assert settings.dedup_text_chars == 1234
    assert settings.dedup_device == "cpu"


def test_app_settings_dedup_defaults(monkeypatch) -> None:
    monkeypatch.setenv("OPENSEARCH_HOSTS", "http://opensearch:9200")
    monkeypatch.delenv("DEDUP_MODEL_NAME", raising=False)
    monkeypatch.delenv("DEDUP_SIMILARITY_THRESHOLD", raising=False)
    monkeypatch.delenv("DEDUP_WINDOW_HOURS", raising=False)
    monkeypatch.delenv("DEDUP_LOOKBACK_HOURS", raising=False)
    monkeypatch.delenv("DEDUP_TEXT_CHARS", raising=False)
    monkeypatch.delenv("DEDUP_DEVICE", raising=False)

    settings = AppSettings.from_env()

    assert settings.dedup_model_name == "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    assert settings.dedup_similarity_threshold == 0.90
    assert settings.dedup_window_hours == 3
    assert settings.dedup_lookback_hours == 24
    assert settings.dedup_text_chars == 1000
    assert settings.dedup_device == "cpu"


def test_default_opensearch_hosts_prefers_explicit_env(monkeypatch) -> None:
    monkeypatch.setenv("OPENSEARCH_HOSTS", "http://custom:9200")

    assert _default_opensearch_hosts() == "http://custom:9200"


def test_default_opensearch_hosts_uses_localhost_outside_docker(monkeypatch) -> None:
    monkeypatch.delenv("OPENSEARCH_HOSTS", raising=False)
    monkeypatch.setattr("news_analyzer.settings.app_settings.Path.exists", lambda self: False)

    assert _default_opensearch_hosts() == "http://localhost:9200"
