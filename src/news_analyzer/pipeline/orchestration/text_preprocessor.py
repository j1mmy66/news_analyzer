from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TruncatedText:
    text: str
    was_truncated: bool
    truncated_count: int
    dropped_count: int
    input_chars: int
    output_chars: int


@dataclass(frozen=True)
class PreparedHourlyTexts:
    texts: list[str]
    was_truncated: bool
    truncated_count: int
    dropped_count: int
    input_chars: int
    output_chars: int


def truncate_text(text: str, max_chars: int) -> TruncatedText:
    source = text or ""
    input_chars = len(source)
    limit = max(0, max_chars)
    output = source[:limit]
    was_truncated = len(output) < input_chars
    return TruncatedText(
        text=output,
        was_truncated=was_truncated,
        truncated_count=1 if was_truncated else 0,
        dropped_count=0,
        input_chars=input_chars,
        output_chars=len(output),
    )


def prepare_hourly_texts(
    texts: list[str],
    per_item_max_chars: int,
    total_max_chars: int,
    *,
    latest_first: bool = True,
) -> PreparedHourlyTexts:
    limit_total = max(0, total_max_chars)
    input_chars = sum(len(value) for value in texts)
    per_item = [truncate_text(value, per_item_max_chars) for value in texts]
    truncated_count = sum(value.truncated_count for value in per_item)
    prepared = [value.text for value in per_item]

    selected: list[str] = []
    selected_chars = 0
    source = reversed(prepared) if latest_first else prepared
    for value in source:
        size = len(value)
        if selected_chars + size > limit_total:
            continue
        selected.append(value)
        selected_chars += size

    if latest_first:
        selected.reverse()

    dropped_count = len(prepared) - len(selected)
    output_chars = sum(len(value) for value in selected)
    was_truncated = truncated_count > 0 or dropped_count > 0
    return PreparedHourlyTexts(
        texts=selected,
        was_truncated=was_truncated,
        truncated_count=truncated_count,
        dropped_count=dropped_count,
        input_chars=input_chars,
        output_chars=output_chars,
    )
