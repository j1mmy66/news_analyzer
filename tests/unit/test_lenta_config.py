from __future__ import annotations

from pathlib import Path

from news_analyzer.sources.lenta.config import LentaCollectorConfig


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_from_sources_file_uses_defaults_when_lenta_section_missing(tmp_path: Path) -> None:
    path = _write(tmp_path / "sources.yaml", "{}\n")

    config = LentaCollectorConfig.from_sources_file(path)

    assert config.rss_url == "https://lenta.ru/rss/news"
    assert config.request_timeout == 20
    assert config.max_retries == 3
    assert config.backoff_seconds == 0.5
    assert config.user_agent is None
    assert config.items_limit == 100


def test_from_sources_file_parses_custom_values(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "sources.yaml",
        """
lenta:
  rss_url: "https://lenta.ru/rss/custom.xml"
  request_timeout: 7
  max_retries: 5
  backoff_seconds: 1.25
  user_agent: "custom-agent"
  items_limit: 42
""",
    )

    config = LentaCollectorConfig.from_sources_file(path)

    assert config.rss_url == "https://lenta.ru/rss/custom.xml"
    assert config.request_timeout == 7
    assert config.max_retries == 5
    assert config.backoff_seconds == 1.25
    assert config.user_agent == "custom-agent"
    assert config.items_limit == 42


def test_from_sources_file_normalizes_empty_url_and_limit(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "sources.yaml",
        """
lenta:
  rss_url: "   "
  items_limit: 0
""",
    )

    config = LentaCollectorConfig.from_sources_file(path)

    assert config.rss_url == "https://lenta.ru/rss/news"
    assert config.items_limit == 1
