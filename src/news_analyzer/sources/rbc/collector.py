from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode, urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from news_analyzer.sources.rbc.config import RBCCollectorConfig

logger = logging.getLogger(__name__)


class RBCFetchError(RuntimeError):
    pass


@dataclass
class RBCCollectStats:
    primary_records: int = 0
    fallback_records: int = 0
    fatal_errors: int = 0
    fetch_errors_total: int = 0
    failed_sections: list[str] = field(default_factory=list)
    fetch_errors_by_section: dict[str, int] = field(default_factory=dict)
    fetch_errors_by_page: dict[str, int] = field(default_factory=dict)

    def note_fetch_error(self, section: str, page: int) -> None:
        self.fetch_errors_total += 1
        self.fetch_errors_by_section[section] = self.fetch_errors_by_section.get(section, 0) + 1
        key = f"{section}:{page}"
        self.fetch_errors_by_page[key] = self.fetch_errors_by_page.get(key, 0) + 1


class RBCNewsCollector:
    BASE_URL = "https://www.rbc.ru"
    SEARCH_URL = "https://www.rbc.ru/search/ajax/"
    DEFAULT_USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    )

    def __init__(self, config: RBCCollectorConfig) -> None:
        self._config = config
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": config.user_agent or self.DEFAULT_USER_AGENT,
                "Accept": "application/json,text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
                "Connection": "keep-alive",
            }
        )
        retry = _build_retry(config)
        self._session.mount("https://", HTTPAdapter(max_retries=retry))
        self._session.mount("http://", HTTPAdapter(max_retries=retry))
        self._last_stats = RBCCollectStats()

    @property
    def last_stats(self) -> RBCCollectStats:
        return self._last_stats

    def collect_latest(self) -> list[dict[str, object]]:
        self._last_stats = RBCCollectStats()

        now = datetime.now(UTC)
        date_to = now.strftime("%d.%m.%Y")
        date_from = (now - timedelta(days=1)).strftime("%d.%m.%Y")

        records: list[dict[str, object]] = []
        for section in self._config.sections:
            section_records, section_failed = self._collect_section(section=section, date_from=date_from, date_to=date_to)
            records.extend(section_records)
            if section_failed:
                self._last_stats.fatal_errors += 1
                self._last_stats.failed_sections.append(section)

        logger.info(
            "RBC collection summary: total=%s primary=%s fallback=%s fetch_errors=%s fatal_sections=%s",
            len(records),
            self._last_stats.primary_records,
            self._last_stats.fallback_records,
            self._last_stats.fetch_errors_total,
            self._last_stats.failed_sections,
        )
        return records

    def _collect_section(self, section: str, date_from: str, date_to: str) -> tuple[list[dict[str, object]], bool]:
        records: list[dict[str, object]] = []
        seen_urls: set[str] = set()

        for page in range(self._config.pages_limit):
            try:
                items = self._fetch_search_items(section=section, date_from=date_from, date_to=date_to, page=page)
                if not items:
                    break
                source = "primary"
            except RBCFetchError as exc:
                self._last_stats.note_fetch_error(section=section, page=page)
                logger.warning("Primary RBC fetch failed section='%s' page=%s: %s", section, page, exc)
                if not self._config.fallback_enabled:
                    logger.error("Fallback is disabled; section '%s' failed", section)
                    return records, True

                fallback_records = self._fetch_fallback_section_records(section=section, date_from=date_from, date_to=date_to)
                if not fallback_records:
                    logger.error("Fallback fetch failed for section '%s'", section)
                    return records, True

                for record in fallback_records:
                    url = str(record.get("url") or "")
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    records.append(record)
                    self._last_stats.fallback_records += 1

                logger.info(
                    "Fallback succeeded for section '%s' with %s records",
                    section,
                    len(fallback_records),
                )
                # Fallback fetch returns section snapshot, no paging continuation.
                break

            for item in items:
                record = self._to_record_from_search_item(item, section=section)
                if record is None:
                    continue

                url = str(record["url"])
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                records.append(record)
                if source == "primary":
                    self._last_stats.primary_records += 1

        return records, False

    def _fetch_search_items(self, section: str, date_from: str, date_to: str, page: int) -> list[dict[str, object]]:
        params = {
            "project": "rbcnews",
            "category": f"TopRbcRu_{section}",
            "dateFrom": date_from,
            "dateTo": date_to,
            "page": str(page),
            "query": "",
            "material": "",
        }
        url = f"{self.SEARCH_URL}?{urlencode(params)}"
        try:
            response = self._session.get(url, timeout=self._config.request_timeout)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:  # noqa: BLE001
            raise RBCFetchError(str(exc)) from exc

        items = payload.get("items", [])
        if not isinstance(items, list):
            return []
        return [item for item in items if isinstance(item, dict)]

    def _fetch_fallback_section_records(self, section: str, date_from: str, date_to: str) -> list[dict[str, object]]:
        url = urljoin(self.BASE_URL, f"/{section}/")
        try:
            response = self._session.get(url, timeout=self._config.request_timeout)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to fetch RBC section page '%s'", section)
            return []

        from_day = datetime.strptime(date_from, "%d.%m.%Y").date()
        to_day = datetime.strptime(date_to, "%d.%m.%Y").date()

        soup = BeautifulSoup(response.text, features="html.parser")
        records: list[dict[str, object]] = []
        seen_urls: set[str] = set()

        for script in soup.find_all("script", {"type": "application/ld+json"}):
            payload = script.string
            if not payload:
                continue
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue

            nodes: list[dict[str, object]] = []
            if isinstance(data, dict):
                nodes = [data]
            elif isinstance(data, list):
                nodes = [value for value in data if isinstance(value, dict)]

            for node in nodes:
                if node.get("@type") != "NewsArticle":
                    continue
                record = self._to_record_from_jsonld(node, section=section)
                if record is None:
                    continue
                published_at = record["published_at"]
                if isinstance(published_at, datetime):
                    published_day: date = published_at.date()
                    if published_day < from_day or published_day > to_day:
                        continue
                url_value = str(record["url"])
                if url_value in seen_urls:
                    continue
                seen_urls.add(url_value)
                records.append(record)

        return records

    def _to_record_from_search_item(self, item: dict[str, object], section: str) -> dict[str, object] | None:
        url = str(item.get("fronturl") or item.get("url") or "").strip()
        title = str(item.get("title") or "").strip()
        if not url or not title:
            return None

        published_at = self._parse_published_at(item)
        if published_at is None:
            return None

        overview, text = self._get_article_data(url)
        body = (text or overview or str(item.get("announce") or "")).strip()
        if not body:
            body = title

        authors_raw = item.get("authors")
        authors = [str(value).strip() for value in authors_raw if str(value).strip()] if isinstance(authors_raw, list) else []

        return {
            "url": url,
            "title": title,
            "body": body,
            "published_at": published_at,
            "authors": authors,
            "section": section,
        }

    def _to_record_from_jsonld(self, data: dict[str, object], section: str) -> dict[str, object] | None:
        url = str(data.get("url") or "").strip()
        title = str(data.get("headline") or "").strip()
        body = str(data.get("articleBody") or "").strip()
        published_raw = str(data.get("datePublished") or "").strip()

        if not url or not title or not published_raw:
            return None

        try:
            published_at = datetime.fromisoformat(published_raw.replace("Z", "+00:00"))
        except ValueError:
            return None

        authors_value = data.get("author")
        authors: list[str] = []
        if isinstance(authors_value, list):
            for author in authors_value:
                if isinstance(author, dict) and author.get("name"):
                    authors.append(str(author["name"]))
        elif isinstance(authors_value, dict) and authors_value.get("name"):
            authors.append(str(authors_value["name"]))

        return {
            "url": url,
            "title": title,
            "body": body or title,
            "published_at": published_at.astimezone(UTC),
            "authors": authors,
            "section": section,
        }

    def _parse_published_at(self, item: dict[str, object]) -> datetime | None:
        candidates = [item.get("publish_date_t"), item.get("publish_date"), item.get("date"), item.get("datetime")]
        for value in candidates:
            if value is None:
                continue

            if isinstance(value, datetime):
                return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)

            if isinstance(value, (int, float)):
                timestamp = float(value)
                if timestamp > 10_000_000_000:
                    timestamp /= 1000
                try:
                    return datetime.fromtimestamp(timestamp, tz=UTC)
                except (OSError, OverflowError, ValueError):
                    continue

            text = str(value).strip()
            if not text:
                continue

            try:
                return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)
            except ValueError:
                pass

            for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(text, fmt).replace(tzinfo=UTC)
                except ValueError:
                    continue

            try:
                parsed = parsedate_to_datetime(text)
                if parsed.tzinfo is None:
                    return parsed.replace(tzinfo=UTC)
                return parsed.astimezone(UTC)
            except (TypeError, ValueError):
                continue

        return None

    def _get_article_data(self, url: str) -> tuple[str | None, str | None]:
        try:
            response = self._session.get(url, timeout=self._config.request_timeout)
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            logger.exception("Failed to fetch RBC article '%s'", url)
            return None, None

        soup = BeautifulSoup(response.text, features="html.parser")

        overview_block = soup.find("div", {"class": "article__text__overview"})
        overview = overview_block.get_text(" ", strip=True) if overview_block else None

        paragraphs = soup.find_all("p")
        filtered = [
            p
            for p in paragraphs
            if not p.find_parent("div", class_="article__special_container")
            and not p.find_parent("div", class_="showcase-collection__subtitle")
            and not p.find_parent("div", class_="showcase-collection-card__text")
            and not p.find_parent("div", class_="showcase-collection__footer")
        ]
        text = " ".join(p.get_text(" ", strip=True) for p in filtered if p.get_text(strip=True)) or None
        return overview, text


def _build_retry(config: RBCCollectorConfig) -> Retry:
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
