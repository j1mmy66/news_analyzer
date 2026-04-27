from __future__ import annotations

from news_analyzer.nlp.dedup.semantic import SemanticNewsDeduplicator


class _FakeEmbeddingModel:
    def __init__(self, vectors: list[list[float]]) -> None:
        self._vectors = vectors
        self.seen_texts: list[str] = []

    def embed(self, texts: list[str]) -> list[list[float]]:
        self.seen_texts = texts
        assert len(texts) == len(self._vectors)
        return self._vectors


def _item(
    external_id: str,
    published_at: str,
    *,
    title: str,
    cleaned_text: str = "",
    raw_text: str = "",
) -> dict[str, object]:
    return {
        "external_id": external_id,
        "published_at": published_at,
        "source_metadata": {"title": title},
        "cleaned_text": cleaned_text,
        "raw_text": raw_text,
    }


def test_deduplicate_marks_latest_item_as_canonical_within_window() -> None:
    model = _FakeEmbeddingModel(vectors=[[1.0, 0.0], [0.95, 0.3122498999]])
    deduplicator = SemanticNewsDeduplicator(model, similarity_threshold=0.90, window_hours=3)

    updates = deduplicator.deduplicate(
        [
            _item("id-1", "2026-03-17T10:00:00+00:00", title="A", cleaned_text="news one"),
            _item("id-2", "2026-03-17T11:00:00+00:00", title="B", cleaned_text="news two"),
        ]
    )

    updates_by_id = {row.external_id: row for row in updates}
    assert updates_by_id["id-2"].is_canonical is True
    assert updates_by_id["id-2"].canonical_external_id == "id-2"
    assert updates_by_id["id-1"].is_canonical is False
    assert updates_by_id["id-1"].canonical_external_id == "id-2"
    assert updates_by_id["id-1"].similarity_to_canonical >= 0.9


def test_deduplicate_does_not_merge_items_outside_window() -> None:
    model = _FakeEmbeddingModel(vectors=[[1.0, 0.0], [1.0, 0.0]])
    deduplicator = SemanticNewsDeduplicator(model, similarity_threshold=0.90, window_hours=3)

    updates = deduplicator.deduplicate(
        [
            _item("id-1", "2026-03-17T10:00:00+00:00", title="A", cleaned_text="news one"),
            _item("id-2", "2026-03-17T14:30:00+00:00", title="B", cleaned_text="news two"),
        ]
    )

    updates_by_id = {row.external_id: row for row in updates}
    assert updates_by_id["id-1"].is_canonical is True
    assert updates_by_id["id-1"].canonical_external_id == "id-1"
    assert updates_by_id["id-2"].is_canonical is True
    assert updates_by_id["id-2"].canonical_external_id == "id-2"


def test_deduplicate_uses_transitive_links_and_tie_break_for_canonical() -> None:
    model = _FakeEmbeddingModel(
        vectors=[
            [1.0, 0.0],
            [0.95, 0.3122498999],
            [0.8, 0.6],
        ]
    )
    deduplicator = SemanticNewsDeduplicator(model, similarity_threshold=0.90, window_hours=3)

    updates = deduplicator.deduplicate(
        [
            _item("id-a", "2026-03-17T10:00:00+00:00", title="A", cleaned_text="a"),
            _item("id-b", "2026-03-17T10:30:00+00:00", title="B", cleaned_text="b"),
            _item("id-c", "2026-03-17T10:30:00+00:00", title="C", cleaned_text="c"),
        ]
    )

    updates_by_id = {row.external_id: row for row in updates}
    assert updates_by_id["id-c"].is_canonical is True
    assert updates_by_id["id-c"].canonical_external_id == "id-c"
    assert updates_by_id["id-a"].canonical_external_id == "id-c"
    assert updates_by_id["id-b"].canonical_external_id == "id-c"


def test_deduplicate_builds_text_from_title_and_first_text_chars() -> None:
    model = _FakeEmbeddingModel(vectors=[[1.0], [1.0]])
    deduplicator = SemanticNewsDeduplicator(model, similarity_threshold=0.90, window_hours=3, text_chars=1000)

    long_cleaned = "x" * 1200
    deduplicator.deduplicate(
        [
            _item("id-1", "2026-03-17T10:00:00+00:00", title="Title", cleaned_text=long_cleaned),
            _item("id-2", "2026-03-17T10:10:00+00:00", title="Fallback", cleaned_text="", raw_text="raw body"),
        ]
    )

    assert model.seen_texts[0] == f"Title {'x' * 1000}"
    assert model.seen_texts[1] == "Fallback raw body"
