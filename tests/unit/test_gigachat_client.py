from __future__ import annotations

import pytest

from news_analyzer.summarization.gigachat.client import (
    GigaChatAuthError,
    GigaChatClient,
    GigaChatResponseFormatError,
    GigaChatTransportError,
)


def test_gigachat_client_parses_chat_completion_content(monkeypatch) -> None:
    def _fake_chat_completion(self: GigaChatClient, prompt: str):
        return {
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "  Краткое саммари.  ",
                    }
                }
            ]
        }

    monkeypatch.setattr(GigaChatClient, "_chat_completion", _fake_chat_completion)

    client = GigaChatClient(auth_key="secret")
    assert client.summarize("prompt") == "Краткое саммари."


def test_gigachat_client_retries_on_retryable_error(monkeypatch) -> None:
    attempts = {"value": 0}

    def _fake_chat_completion(self: GigaChatClient, prompt: str):
        attempts["value"] += 1
        if attempts["value"] < 3:
            raise GigaChatTransportError("temporary network error")
        return {"choices": [{"message": {"content": "ok"}}]}

    monkeypatch.setattr(GigaChatClient, "_chat_completion", _fake_chat_completion)
    monkeypatch.setattr("news_analyzer.summarization.gigachat.client.time.sleep", lambda _: None)

    client = GigaChatClient(auth_key="secret", max_retries=3, backoff_base_seconds=0)
    assert client.summarize("prompt") == "ok"
    assert attempts["value"] == 3


def test_gigachat_client_does_not_retry_on_auth_error(monkeypatch) -> None:
    attempts = {"value": 0}

    def _fake_chat_completion(self: GigaChatClient, prompt: str):
        attempts["value"] += 1
        raise GigaChatAuthError("unauthorized")

    monkeypatch.setattr(GigaChatClient, "_chat_completion", _fake_chat_completion)

    client = GigaChatClient(auth_key="secret", max_retries=3)

    with pytest.raises(GigaChatAuthError):
        client.summarize("prompt")

    assert attempts["value"] == 1


def test_gigachat_client_raises_on_invalid_response(monkeypatch) -> None:
    monkeypatch.setattr(GigaChatClient, "_chat_completion", lambda self, prompt: {"choices": []})

    client = GigaChatClient(auth_key="secret")

    with pytest.raises(GigaChatResponseFormatError):
        client.summarize("prompt")
