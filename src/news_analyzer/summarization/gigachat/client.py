from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


class GigaChatError(RuntimeError):
    pass


class GigaChatDependencyError(GigaChatError):
    pass


class GigaChatAuthError(GigaChatError):
    pass


class GigaChatRateLimitError(GigaChatError):
    pass


class GigaChatValidationError(GigaChatError):
    pass


class GigaChatServerError(GigaChatError):
    pass


class GigaChatTransportError(GigaChatError):
    pass


class GigaChatResponseFormatError(GigaChatError):
    pass


@dataclass(frozen=True)
class GigaChatClient:
    auth_key: str
    scope: str = "GIGACHAT_API_PERS"
    model: str = "GigaChat"
    timeout_seconds: float = 15.0
    max_retries: int = 3
    backoff_base_seconds: float = 0.5
    verify_ssl: bool = True

    _SYSTEM_PROMPT = "Ты редактор новостей. Отвечай кратко, по фактам, без домыслов."

    def summarize(self, prompt: str) -> str:
        if not prompt.strip():
            raise GigaChatValidationError("Prompt is empty")

        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._call(prompt)
            except GigaChatError as exc:
                last_error = exc
                if attempt == self.max_retries or not self._is_retryable(exc):
                    break
                sleep_seconds = min(self.backoff_base_seconds * (2 ** (attempt - 1)), 5.0)
                logger.warning("GigaChat request failed, retrying in %.2fs", sleep_seconds)
                time.sleep(sleep_seconds)
        if isinstance(last_error, GigaChatError):
            raise last_error
        raise GigaChatError("Unknown GigaChat failure")

    def _call(self, prompt: str) -> str:
        try:
            payload = self._chat_completion(prompt)
        except GigaChatError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise self._map_exception(exc) from exc

        summary = self._extract_content(payload)
        if not summary:
            raise GigaChatResponseFormatError("Invalid GigaChat response format")
        return summary

    def _chat_completion(self, prompt: str) -> Any:
        try:
            from gigachat import GigaChat
        except ImportError as exc:
            raise GigaChatDependencyError("gigachat package is not installed") from exc

        init_kwargs: dict[str, Any] = {
            "credentials": self.auth_key,
            "scope": self.scope,
            "timeout": self.timeout_seconds,
            "verify_ssl_certs": self.verify_ssl,
        }
        try:
            with GigaChat(**init_kwargs) as giga:
                return giga.chat(
                    {
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": self._SYSTEM_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                        "stream": False,
                    }
                )
        except TypeError:
            # Backward compatibility with older SDK argument name.
            init_kwargs.pop("verify_ssl_certs")
            init_kwargs["verify_ssl"] = self.verify_ssl
            with GigaChat(**init_kwargs) as giga:
                return giga.chat(
                    {
                        "model": self.model,
                        "messages": [
                            {"role": "system", "content": self._SYSTEM_PROMPT},
                            {"role": "user", "content": prompt},
                        ],
                        "stream": False,
                    }
                )

    def _extract_content(self, payload: Any) -> str | None:
        if isinstance(payload, dict):
            choices = payload.get("choices")
        else:
            choices = getattr(payload, "choices", None)
        if not choices:
            return None

        first_choice = choices[0]
        if isinstance(first_choice, dict):
            message = first_choice.get("message", {})
        else:
            message = getattr(first_choice, "message", None)

        if isinstance(message, dict):
            content = message.get("content")
        else:
            content = getattr(message, "content", None)

        if not isinstance(content, str):
            return None
        normalized = content.strip()
        return normalized or None

    def _map_exception(self, exc: Exception) -> GigaChatError:
        message = str(exc).lower()
        if "401" in message or "unauthor" in message or "token" in message:
            return GigaChatAuthError(str(exc))
        if "429" in message or "too many requests" in message:
            return GigaChatRateLimitError(str(exc))
        if "422" in message or "invalid params" in message:
            return GigaChatValidationError(str(exc))
        if "500" in message or "502" in message or "503" in message or "504" in message:
            return GigaChatServerError(str(exc))
        return GigaChatTransportError(str(exc))

    def _is_retryable(self, exc: GigaChatError) -> bool:
        return isinstance(
            exc,
            (
                GigaChatRateLimitError,
                GigaChatServerError,
                GigaChatTransportError,
            ),
        )
