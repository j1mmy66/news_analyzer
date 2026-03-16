from __future__ import annotations

from news_analyzer.settings.app_settings import AppSettings


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
