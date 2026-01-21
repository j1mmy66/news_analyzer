from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)


class GigaChatError(RuntimeError):
    pass


@dataclass(frozen=True)
class GigaChatClient:
    base_url: str
    api_key: str
    timeout_seconds: float = 15.0
    max_retries: int = 3
    backoff_base_seconds: float = 0.5

    def summarize(self, prompt: str) -> str:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                return self._call(prompt)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.max_retries:
                    break
                sleep_seconds = min(self.backoff_base_seconds * (2 ** (attempt - 1)), 5.0)
                logger.warning("GigaChat request failed, retrying in %.2fs", sleep_seconds)
                time.sleep(sleep_seconds)
        raise GigaChatError(str(last_error) if last_error else "Unknown GigaChat failure")

    def _call(self, prompt: str) -> str:
        req = Request(
            url=self.base_url,
            data=json.dumps({"prompt": prompt}).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        with urlopen(req, timeout=self.timeout_seconds) as response:  # noqa: S310
            payload = json.loads(response.read().decode("utf-8"))

        summary = payload.get("summary")
        if not isinstance(summary, str) or not summary.strip():
            raise GigaChatError("Invalid GigaChat response format")

        return summary.strip()
