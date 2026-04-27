from __future__ import annotations

from pathlib import Path

import pytest

from news_analyzer.sources.rbc.config import RBCCollectorConfig, _to_bool


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def test_from_sources_file_uses_defaults_when_rbc_section_missing(tmp_path: Path) -> None:
    path = _write(tmp_path / "sources.yaml", "{}\n")

    config = RBCCollectorConfig.from_sources_file(path)

    assert config.sections == ["economics"]
    assert config.request_timeout == 20
    assert config.pages_limit == 2
    assert config.max_retries == 3
    assert config.backoff_seconds == 0.5
    assert config.fallback_enabled is True
    assert config.user_agent is None


def test_from_sources_file_parses_custom_values(tmp_path: Path) -> None:
    path = _write(
        tmp_path / "sources.yaml",
        """
rbc:
  sections: [economics, politics, "  "]
  request_timeout: 7
  pages_limit: 4
  max_retries: 5
  backoff_seconds: 1.25
  fallback_enabled: "off"
  user_agent: "custom-agent"
""",
    )

    config = RBCCollectorConfig.from_sources_file(path)

    assert config.sections == ["economics", "politics"]
    assert config.request_timeout == 7
    assert config.pages_limit == 4
    assert config.max_retries == 5
    assert config.backoff_seconds == 1.25
    assert config.fallback_enabled is False
    assert config.user_agent == "custom-agent"


def test_from_sources_file_raises_when_sections_is_not_list(tmp_path: Path) -> None:
    path = _write(tmp_path / "sources.yaml", "rbc:\n  sections: economics\n")

    with pytest.raises(ValueError, match="rbc.sections must be a list"):
        RBCCollectorConfig.from_sources_file(path)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (True, True),
        (False, False),
        ("true", True),
        ("yes", True),
        ("on", True),
        ("1", True),
        ("false", False),
        ("no", False),
        ("off", False),
        ("0", False),
        ("  unknown  ", True),
        (0, False),
        (2, True),
        (None, False),
    ],
)
def test_to_bool(value: object, expected: bool) -> None:
    assert _to_bool(value) is expected
