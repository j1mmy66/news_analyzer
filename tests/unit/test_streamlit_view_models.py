from __future__ import annotations

from news_analyzer.apps.streamlit.view_models import NewsCursor


def test_news_cursor_from_sort_returns_none_for_short_sort() -> None:
    assert NewsCursor.from_sort(["2026-03-17T08:00:00+00:00"]) is None


def test_news_cursor_from_sort_returns_none_for_unsupported_values() -> None:
    assert NewsCursor.from_sort([True, "id-1"]) is None
    assert NewsCursor.from_sort(["2026-03-17T08:00:00+00:00", 999]) is None


def test_news_cursor_to_search_after_and_from_sort_success() -> None:
    cursor = NewsCursor.from_sort(["2026-03-17T08:00:00+00:00", "id-1"])
    assert cursor is not None
    assert cursor.to_search_after() == ["2026-03-17T08:00:00+00:00", "id-1"]


def test_news_cursor_from_sort_supports_epoch_millis() -> None:
    cursor = NewsCursor.from_sort([1710748800000, "id-1"])
    assert cursor is not None
    assert cursor.to_search_after() == [1710748800000, "id-1"]
