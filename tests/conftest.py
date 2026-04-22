from __future__ import annotations

from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests (requires Docker).",
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "integration: mark test as integration (requires --run-integration).",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    run_integration = config.getoption("--run-integration")
    skip_marker = pytest.mark.skip(reason="integration tests are skipped by default; use --run-integration")

    for item in items:
        item_path = Path(str(getattr(item, "path", item.fspath))).as_posix()
        if "/tests/integration/" in f"/{item_path}":
            item.add_marker(pytest.mark.integration)

        if item.get_closest_marker("integration") and not run_integration:
            item.add_marker(skip_marker)
