from __future__ import annotations

from pathlib import Path
from typing import Any

from news_analyzer.domain.models import Entity
from news_analyzer.nlp.ner.interface import NERModel


class NatashaSlovnetNERModel(NERModel):
    """NER implementation based on Slovnet with Natasha normalization."""

    def __init__(self, slovnet_model_path: Path, navec_path: Path) -> None:
        self._slovnet_model_path = slovnet_model_path
        self._navec_path = navec_path
        self._validate_paths()
        self._ner, self._doc_factory, self._segmenter, self._morph_vocab = self._build_runtime()

    def _validate_paths(self) -> None:
        if not self._slovnet_model_path.is_file():
            raise FileNotFoundError(f"Slovnet model not found: {self._slovnet_model_path}")
        if not self._navec_path.is_file():
            raise FileNotFoundError(f"Navec model not found: {self._navec_path}")

    def _build_runtime(self) -> tuple[Any, Any, Any, Any]:
        try:
            from natasha import Doc, MorphVocab, Segmenter
            from navec import Navec
            from slovnet import NER
        except ImportError as exc:  # pragma: no cover - depends on optional runtime packages
            raise RuntimeError(
                "Natasha/Slovnet dependencies are missing. Install requirements before running NER."
            ) from exc

        navec = Navec.load(str(self._navec_path))
        ner = NER.load(str(self._slovnet_model_path))
        ner.navec(navec)
        return ner, Doc, Segmenter(), MorphVocab()

    def _normalize_span(self, value: str) -> str | None:
        try:
            doc = self._doc_factory(value)
            doc.segment(self._segmenter)
            if not getattr(doc, "tokens", None):
                return None

            lemmas: list[str] = []
            for token in doc.tokens:
                if hasattr(token, "lemmatize"):
                    token.lemmatize(self._morph_vocab)
                lemma = getattr(token, "lemma", None)
                if lemma:
                    lemmas.append(str(lemma))
            if not lemmas:
                return None
            return " ".join(lemmas)
        except Exception:  # noqa: BLE001
            return None

    def extract(self, text: str) -> list[Entity]:
        if not text.strip():
            return []

        markup = self._ner(text)
        entities: list[Entity] = []
        for span in getattr(markup, "spans", []):
            start = int(getattr(span, "start"))
            end = int(getattr(span, "stop"))
            entity_text = text[start:end]
            entities.append(
                Entity(
                    text=entity_text,
                    label=str(getattr(span, "type")),
                    start=start,
                    end=end,
                    confidence=float(getattr(span, "score", 1.0)),
                    normalized=self._normalize_span(entity_text),
                )
            )
        return entities
