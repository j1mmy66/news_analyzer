from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC
from email.utils import parsedate_to_datetime
import xml.etree.ElementTree as ET

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from news_analyzer.sources.lenta.config import LentaCollectorConfig

logger = logging.getLogger(__name__)


class LentaFetchError(RuntimeError):
    pass


@dataclass
class LentaCollectStats:
    fetched: int = 0
    parsed: int = 0
    full_text_ok: int = 0
    skipped_no_full_text: int = 0
    skipped_fetch_error: int = 0
    skipped_challenge: int = 0
    skipped_empty_text: int = 0
    fetch_errors: int = 0
    fatal_errors: int = 0


class LentaNewsCollector:
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    def __init__(self, config: LentaCollectorConfig) -> None:
        self._config = config
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": config.user_agent or self.DEFAULT_USER_AGENT,
                "Accept": "application/rss+xml,application/xml,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
                "Connection": "keep-alive",
            }
        )
        retry = _build_retry(config)
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._last_stats = LentaCollectStats()

    @property
    def last_stats(self) -> LentaCollectStats:
        return self._last_stats

    def collect_latest(self) -> list[dict[str, object]]:
        self._last_stats = LentaCollectStats()

        try:
            rss_items = self._fetch_rss_items()
        except LentaFetchError as exc:
            self._last_stats.fetch_errors += 1
            self._last_stats.fatal_errors += 1
            logger.error("Lenta RSS fetch failed: %s", exc)
            return []

        self._last_stats.fetched = len(rss_items)
        records: list[dict[str, object]] = []
        seen_urls: set[str] = set()

        for item in rss_items:
            record = self._to_record_from_rss_item(item)
            if record is None:
                continue

            self._last_stats.parsed += 1
            url = str(record["url"])
            if url in seen_urls:
                continue
            seen_urls.add(url)

            full_text, skip_reason = self._fetch_full_text(url)
            if not full_text:
                self._last_stats.skipped_no_full_text += 1
                if skip_reason == "fetch_error":
                    self._last_stats.skipped_fetch_error += 1
                elif skip_reason == "challenge_block":
                    self._last_stats.skipped_challenge += 1
                else:
                    self._last_stats.skipped_empty_text += 1
                logger.info("Skipping Lenta article without full_text: %s", url)
                continue

            record["body"] = full_text
            records.append(record)
            self._last_stats.full_text_ok += 1

            if len(records) >= self._config.items_limit:
                break

        logger.info(
            "Lenta collection summary: fetched=%s parsed=%s full_text_ok=%s skipped_no_full_text=%s "
            "skip_fetch_error=%s skip_challenge=%s skip_empty=%s fetch_errors=%s fatal=%s",
            self._last_stats.fetched,
            self._last_stats.parsed,
            self._last_stats.full_text_ok,
            self._last_stats.skipped_no_full_text,
            self._last_stats.skipped_fetch_error,
            self._last_stats.skipped_challenge,
            self._last_stats.skipped_empty_text,
            self._last_stats.fetch_errors,
            self._last_stats.fatal_errors,
        )
        return records

    def _fetch_rss_items(self) -> list[ET.Element]:
        try:
            response = self._session.get(self._config.rss_url, timeout=self._config.request_timeout)
            response.raise_for_status()
            root = ET.fromstring(response.text)
        except Exception as exc:  # noqa: BLE001
            raise LentaFetchError(str(exc)) from exc

        channel = root.find("channel")
        if channel is None:
            return []
        return list(channel.findall("item"))

    def _to_record_from_rss_item(self, item: ET.Element) -> dict[str, object] | None:
        title = (item.findtext("title") or "").strip()
        url = (item.findtext("link") or "").strip()
        pub_date = (item.findtext("pubDate") or "").strip()
        description = (item.findtext("description") or "").strip()
        categories = [(node.text or "").strip() for node in item.findall("category")]
        categories = [value for value in categories if value]

        if not title or not url or not pub_date:
            return None

        try:
            published_at = parsedate_to_datetime(pub_date)
        except (TypeError, ValueError):
            return None

        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=UTC)

        section = categories[0] if categories else "lenta"

        return {
            "url": url,
            "title": title,
            "body": description,
            "published_at": published_at.astimezone(UTC),
            "authors": [],
            "section": section,
        }

    def _fetch_full_text(self, url: str) -> tuple[str | None, str]:
        try:
            response = self._session.get(url, timeout=self._config.request_timeout)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            self._last_stats.fetch_errors += 1
            logger.exception("Failed to fetch Lenta article '%s'", url)
            return None, "fetch_error"

        text, reason = self._extract_full_text(response.text)
        return (text if text else None), reason

    def _extract_full_text(self, html: str) -> tuple[str | None, str]:
        if not html:
            return None, "empty_text"

        lowered = html.lower()
        if "captcha" in lowered or "cloudflare" in lowered or "ddos-guard" in lowered:
            return None, "challenge_block"

        soup = BeautifulSoup(html, features="html.parser")

        selectors = (
            "[itemprop='articleBody']",
            "div.topic-body__content",
            "div.topic-body",
            "article",
        )
        for selector in selectors:
            for node in soup.select(selector):
                text = node.get_text(" ", strip=True)
                if text:
                    return " ".join(text.split()), "ok"

        paragraphs = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
        cleaned = [p for p in paragraphs if p]
        if not cleaned:
            return None, "empty_text"

        text = " ".join(cleaned)
        normalized = " ".join(text.split()) or None
        return normalized, "ok" if normalized else "empty_text"


def _build_retry(config: LentaCollectorConfig) -> Retry:
    retries = max(0, int(config.max_retries))
    return Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=max(0.0, float(config.backoff_seconds)),
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
