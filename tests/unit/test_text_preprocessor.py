from news_analyzer.pipeline.orchestration.text_preprocessor import prepare_hourly_texts, truncate_text


def test_truncate_text_returns_original_when_within_limit() -> None:
    result = truncate_text("abcd", max_chars=10)

    assert result.text == "abcd"
    assert result.was_truncated is False
    assert result.truncated_count == 0
    assert result.dropped_count == 0
    assert result.input_chars == 4
    assert result.output_chars == 4


def test_truncate_text_uses_head_only_limit() -> None:
    result = truncate_text("abcdefgh", max_chars=5)

    assert result.text == "abcde"
    assert result.was_truncated is True
    assert result.truncated_count == 1
    assert result.dropped_count == 0
    assert result.input_chars == 8
    assert result.output_chars == 5


def test_prepare_hourly_texts_applies_per_item_and_total_latest_first() -> None:
    result = prepare_hourly_texts(
        ["1111", "2222", "3333"],
        per_item_max_chars=3,
        total_max_chars=6,
        latest_first=True,
    )

    assert result.texts == ["222", "333"]
    assert result.was_truncated is True
    assert result.truncated_count == 3
    assert result.dropped_count == 1
    assert result.input_chars == 12
    assert result.output_chars == 6


def test_prepare_hourly_texts_handles_empty_items() -> None:
    result = prepare_hourly_texts(
        ["", ""],
        per_item_max_chars=3,
        total_max_chars=10,
        latest_first=True,
    )

    assert result.texts == ["", ""]
    assert result.was_truncated is False
    assert result.truncated_count == 0
    assert result.dropped_count == 0
    assert result.input_chars == 0
    assert result.output_chars == 0
