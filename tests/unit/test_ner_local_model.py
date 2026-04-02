from __future__ import annotations

from pathlib import Path
import types

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


def test_model_validates_missing_navec_path(tmp_path: Path) -> None:
    model_path = tmp_path / "slovnet.tar"
    _touch(model_path)

    with pytest.raises(FileNotFoundError, match="Navec model not found"):
        NatashaSlovnetNERModel(model_path, tmp_path / "missing-navec.tar")


def test_extract_returns_empty_for_blank_text(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    monkeypatch.setattr(
        NatashaSlovnetNERModel,
        "_build_runtime",
        lambda self: (lambda text: _FakeMarkup([]), _FakeDoc, object(), object()),
    )

    model = NatashaSlovnetNERModel(model_path, navec_path)
    assert model.extract("   ") == []


def test_normalize_span_returns_none_when_no_tokens(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    class _DocNoTokens:
        def __init__(self, text: str) -> None:
            self.tokens = []

        def segment(self, _segmenter: object) -> None:
            return None

    monkeypatch.setattr(
        NatashaSlovnetNERModel,
        "_build_runtime",
        lambda self: (lambda text: _FakeMarkup([]), _DocNoTokens, object(), object()),
    )

    model = NatashaSlovnetNERModel(model_path, navec_path)
    assert model._normalize_span("Москва") is None


def test_normalize_span_returns_none_on_exception(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    class _DocRaises:
        def __init__(self, text: str) -> None:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        NatashaSlovnetNERModel,
        "_build_runtime",
        lambda self: (lambda text: _FakeMarkup([]), _DocRaises, object(), object()),
    )

    model = NatashaSlovnetNERModel(model_path, navec_path)
    assert model._normalize_span("Москва") is None


def test_build_runtime_with_fake_optional_dependencies(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    class _FakeNavecObj:
        pass

    class _FakeNavec:
        @staticmethod
        def load(path: str):
            assert path == str(navec_path)
            return _FakeNavecObj()

    class _FakeNERObj:
        def __init__(self) -> None:
            self.navec_value = None

        def navec(self, value: object) -> None:
            self.navec_value = value

    fake_ner_obj = _FakeNERObj()

    class _FakeNER:
        @staticmethod
        def load(path: str):
            assert path == str(model_path)
            return fake_ner_obj

    class _FakeSegmenter:
        pass

    class _FakeMorphVocab:
        pass

    class _FakeDocRuntime:
        def __init__(self, text: str) -> None:
            self.text = text

    monkeypatch.setitem(
        __import__("sys").modules,
        "natasha",
        types.SimpleNamespace(Doc=_FakeDocRuntime, MorphVocab=_FakeMorphVocab, Segmenter=_FakeSegmenter),
    )
    monkeypatch.setitem(__import__("sys").modules, "navec", types.SimpleNamespace(Navec=_FakeNavec))
    monkeypatch.setitem(__import__("sys").modules, "slovnet", types.SimpleNamespace(NER=_FakeNER))

    model = object.__new__(NatashaSlovnetNERModel)
    model._slovnet_model_path = model_path
    model._navec_path = navec_path

    ner, doc_factory, segmenter, morph_vocab = NatashaSlovnetNERModel._build_runtime(model)

    assert ner is fake_ner_obj
    assert doc_factory is _FakeDocRuntime
    assert isinstance(segmenter, _FakeSegmenter)
    assert isinstance(morph_vocab, _FakeMorphVocab)
    assert fake_ner_obj.navec_value is not None


def test_normalize_span_returns_none_when_lemmas_are_missing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    _touch(model_path)
    _touch(navec_path)

    class _TokenWithoutLemma:
        def lemmatize(self, _morph_vocab: object) -> None:
            return None

    class _DocWithoutLemmas:
        def __init__(self, text: str) -> None:
            self.tokens = [_TokenWithoutLemma()]

        def segment(self, _segmenter: object) -> None:
            return None

    monkeypatch.setattr(
        NatashaSlovnetNERModel,
        "_build_runtime",
        lambda self: (lambda text: _FakeMarkup([]), _DocWithoutLemmas, object(), object()),
    )

    model = NatashaSlovnetNERModel(model_path, navec_path)
    assert model._normalize_span("Москва") is None
