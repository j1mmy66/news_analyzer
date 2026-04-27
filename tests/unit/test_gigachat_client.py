from __future__ import annotations

import types

import pytest

from news_analyzer.summarization.gigachat.client import (
    GigaChatAuthError,
    GigaChatClient,
    GigaChatDependencyError,
    GigaChatError,
    GigaChatRateLimitError,
    GigaChatResponseFormatError,
    GigaChatServerError,
    GigaChatTransportError,
    GigaChatValidationError,
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


def test_gigachat_client_rejects_empty_prompt() -> None:
    client = GigaChatClient(auth_key="secret")

    with pytest.raises(GigaChatValidationError, match="Prompt is empty"):
        client.summarize("   ")


def test_gigachat_client_backoff_is_capped_to_five_seconds(monkeypatch) -> None:
    sleeps: list[float] = []

    def _always_fail(self: GigaChatClient, prompt: str):
        raise GigaChatRateLimitError("429")

    monkeypatch.setattr(GigaChatClient, "_chat_completion", _always_fail)
    monkeypatch.setattr("news_analyzer.summarization.gigachat.client.time.sleep", lambda seconds: sleeps.append(seconds))

    client = GigaChatClient(auth_key="secret", max_retries=5, backoff_base_seconds=10)

    with pytest.raises(GigaChatRateLimitError):
        client.summarize("prompt")

    assert sleeps == [5.0, 5.0, 5.0, 5.0]


def test_gigachat_client_does_not_retry_on_validation_error(monkeypatch) -> None:
    attempts = {"value": 0}

    def _fail_with_validation(self: GigaChatClient, prompt: str):
        attempts["value"] += 1
        raise GigaChatValidationError("422 invalid params")

    monkeypatch.setattr(GigaChatClient, "_chat_completion", _fail_with_validation)
    client = GigaChatClient(auth_key="secret", max_retries=4)

    with pytest.raises(GigaChatValidationError):
        client.summarize("prompt")

    assert attempts["value"] == 1


def test_extract_content_accepts_object_style_payload() -> None:
    class _Msg:
        content = "  object payload  "

    class _Choice:
        message = _Msg()

    class _Payload:
        choices = [_Choice()]

    client = GigaChatClient(auth_key="secret")
    assert client._extract_content(_Payload()) == "object payload"


def test_extract_content_returns_none_for_invalid_payload() -> None:
    client = GigaChatClient(auth_key="secret")
    assert client._extract_content({"choices": [{"message": {"content": 123}}]}) is None
    assert client._extract_content({"choices": []}) is None


@pytest.mark.parametrize(
    ("message", "error_type"),
    [
        ("401 unauthorized", GigaChatAuthError),
        ("429 too many requests", GigaChatRateLimitError),
        ("422 invalid params", GigaChatValidationError),
        ("503 service unavailable", GigaChatServerError),
        ("socket timeout", GigaChatTransportError),
    ],
)
def test_map_exception_by_message(message: str, error_type: type[Exception]) -> None:
    client = GigaChatClient(auth_key="secret")
    mapped = client._map_exception(RuntimeError(message))
    assert isinstance(mapped, error_type)


def test_is_retryable_matrix() -> None:
    client = GigaChatClient(auth_key="secret")
    assert client._is_retryable(GigaChatRateLimitError("429")) is True
    assert client._is_retryable(GigaChatServerError("503")) is True
    assert client._is_retryable(GigaChatTransportError("network")) is True
    assert client._is_retryable(GigaChatAuthError("401")) is False
    assert client._is_retryable(GigaChatValidationError("422")) is False


def test_chat_completion_fallbacks_to_verify_ssl_when_old_sdk_signature(monkeypatch) -> None:
    init_calls: list[dict[str, object]] = []

    class _FakeGigaChat:
        def __init__(self, **kwargs) -> None:
            init_calls.append(kwargs)
            if "verify_ssl_certs" in kwargs:
                raise TypeError("unexpected keyword argument 'verify_ssl_certs'")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def chat(self, payload):
            return {"choices": [{"message": {"content": "ok"}}]}

    monkeypatch.setitem(__import__("sys").modules, "gigachat", types.SimpleNamespace(GigaChat=_FakeGigaChat))

    client = GigaChatClient(auth_key="secret", verify_ssl=False)
    result = client._chat_completion("prompt")

    assert result["choices"][0]["message"]["content"] == "ok"
    assert len(init_calls) == 2
    assert "verify_ssl_certs" in init_calls[0]
    assert "verify_ssl" in init_calls[1]


def test_summarize_raises_unknown_failure_when_max_retries_is_zero() -> None:
    client = GigaChatClient(auth_key="secret", max_retries=0)

    with pytest.raises(GigaChatError, match="Unknown GigaChat failure"):
        client.summarize("prompt")


def test_call_maps_generic_exception_to_transport_error(monkeypatch) -> None:
    monkeypatch.setattr(GigaChatClient, "_chat_completion", lambda self, prompt: (_ for _ in ()).throw(RuntimeError("socket")))
    client = GigaChatClient(auth_key="secret")

    with pytest.raises(GigaChatTransportError):
        client._call("prompt")


def test_chat_completion_normal_path_with_verify_ssl_certs(monkeypatch) -> None:
    init_calls: list[dict[str, object]] = []

    class _FakeGigaChat:
        def __init__(self, **kwargs) -> None:
            init_calls.append(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return None

        def chat(self, payload):
            return {"choices": [{"message": {"content": "ok-normal"}}]}

    monkeypatch.setitem(__import__("sys").modules, "gigachat", types.SimpleNamespace(GigaChat=_FakeGigaChat))
    client = GigaChatClient(auth_key="secret", verify_ssl=True)

    payload = client._chat_completion("prompt")

    assert payload["choices"][0]["message"]["content"] == "ok-normal"
    assert len(init_calls) == 1
    assert init_calls[0]["verify_ssl_certs"] is True


def test_chat_completion_raises_dependency_error_when_package_missing(monkeypatch) -> None:
    import builtins

    original_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "gigachat":
            raise ImportError("missing gigachat")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)
    client = GigaChatClient(auth_key="secret")

    with pytest.raises(GigaChatDependencyError, match="gigachat package is not installed"):
        client._chat_completion("prompt")
