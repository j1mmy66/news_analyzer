from __future__ import annotations

from pathlib import Path

import pytest

from news_analyzer.nlp.ner.local_model import NatashaSlovnetNERModel


class _FakeSpan:
    def __init__(self, start: int, stop: int, label: str, score: float | None = None) -> None:
        self.start = start
        self.stop = stop
        self.type = label
        if score is not None:
            self.score = score


class _FakeMarkup:
    def __init__(self, spans: list[_FakeSpan]) -> None:
        self.spans = spans


class _FakeToken:
    def __init__(self, value: str) -> None:
        self.text = value
        self.lemma: str | None = None

    def lemmatize(self, _morph_vocab: object) -> None:
        self.lemma = self.text.lower()


class _FakeDoc:
    def __init__(self, text: str) -> None:
        self._text = text
        self.tokens: list[_FakeToken] = []

    def segment(self, _segmenter: object) -> None:
        self.tokens = [_FakeToken(part.strip(".,:;!?")) for part in self._text.split() if part.strip(".,:;!?")]


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("stub", encoding="utf-8")


def test_model_validates_required_paths(tmp_path: Path) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(navec_path)

    with pytest.raises(FileNotFoundError):
        NatashaSlovnetNERModel(model_path, navec_path)


def test_extract_returns_native_labels_and_normalized_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    class _FakeNER:
        def __call__(self, text: str) -> _FakeMarkup:
            return _FakeMarkup(
                [
                    _FakeSpan(text.index("Иван Иванов"), text.index("Иван Иванов") + len("Иван Иванов"), "PER", 0.91),
                    _FakeSpan(text.index("Яндекс"), text.index("Яндекс") + len("Яндекс"), "ORG"),
                ]
            )

    monkeypatch.setattr(
        NatashaSlovnetNERModel,
        "_build_runtime",
        lambda self: (_FakeNER(), _FakeDoc, object(), object()),
    )

    model = NatashaSlovnetNERModel(model_path, navec_path)
    entities = model.extract("Иван Иванов перешел в Яндекс.")

    assert [entity.label for entity in entities] == ["PER", "ORG"]
    assert [entity.text for entity in entities] == ["Иван Иванов", "Яндекс"]
    assert entities[0].normalized == "иван иванов"
    assert entities[1].normalized == "яндекс"
    assert entities[0].confidence == 0.91
    assert entities[1].confidence == 1.0


def test_extract_sets_none_when_normalization_fails(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    class _FakeNER:
        def __call__(self, _text: str) -> _FakeMarkup:
            return _FakeMarkup([_FakeSpan(0, 5, "LOC")])

    monkeypatch.setattr(
        NatashaSlovnetNERModel,
        "_build_runtime",
        lambda self: (_FakeNER(), _FakeDoc, object(), object()),
    )
    monkeypatch.setattr(NatashaSlovnetNERModel, "_normalize_span", lambda self, value: None)

    model = NatashaSlovnetNERModel(model_path, navec_path)
    entities = model.extract("Москва")

    assert len(entities) == 1
    assert entities[0].normalized is None
