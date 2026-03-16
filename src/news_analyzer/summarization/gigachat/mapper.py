from __future__ import annotations


def build_item_prompt(text: str) -> str:
    return (
        "Сделай краткую новостную выжимку (2-3 предложения) на русском языке. "
        "Сохрани факты и не добавляй домыслов.\n\n"
        f"Текст:\n{text}"
    )


def build_hourly_prompt(texts: list[str]) -> str:
    joined = "\n\n---\n\n".join(texts)
    return (
        "Сделай агрегированное summary по новостям за последний час "
        "(5-7 предложений, ключевые темы и события).\n\n"
        f"Новости:\n{joined}"
    )
