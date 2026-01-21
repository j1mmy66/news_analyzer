from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from urllib.parse import urljoin
from urllib.request import urlopen

from news_analyzer.sources.rbc.config import RBCCollectorConfig

logger = logging.getLogger(__name__)


class RBCNewsCollector:
    BASE_URL = "https://www.rbc.ru"

    def __init__(self, config: RBCCollectorConfig) -> None:
        self._config = config

    def collect_latest(self) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        for section in self._config.sections:
            records.extend(self._collect_section(section))
        return records

    def _collect_section(self, section: str) -> list[dict[str, object]]:
        url = urljoin(self.BASE_URL, f"/{section}/")
        try:
            with urlopen(url, timeout=self._config.request_timeout) as response:  # noqa: S310
                html = response.read().decode("utf-8", errors="ignore")
        except Exception:  # noqa: BLE001
            logger.exception("Failed to fetch RBC section '%s'", section)
            return []

        records: list[dict[str, object]] = []
        marker = '"@type":"NewsArticle"'
        for chunk in html.split("<script type=\"application/ld+json\">"):
            if marker not in chunk:
                continue
            payload = chunk.split("</script>", 1)[0]
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                rec = _to_record(data, section)
                if rec:
                    records.append(rec)
        return records


def _to_record(data: dict[str, object], section: str) -> dict[str, object] | None:
    url = str(data.get("url") or "")
    title = str(data.get("headline") or "")
    body = str(data.get("articleBody") or "")
    published_raw = str(data.get("datePublished") or "")

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
        "body": body,
        "published_at": published_at.astimezone(timezone.utc),
        "authors": authors,
        "section": section,
    }
