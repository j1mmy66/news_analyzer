from __future__ import annotations

from pathlib import Path
from typing import Any

from news_analyzer.domain.enums import ClassLabel
from news_analyzer.domain.models import ClassificationResult
from news_analyzer.nlp.classification.interface import ClassificationModel

MODEL_VERSION = "hf-any-news-v1"
HF_LABEL_MAP: dict[str, ClassLabel] = {
    "LABEL_0": ClassLabel.CLIMATE,
    "LABEL_1": ClassLabel.CONFLICTS,
    "LABEL_2": ClassLabel.CULTURE,
    "LABEL_3": ClassLabel.ECONOMY,
    "LABEL_4": ClassLabel.GLOSS,
    "LABEL_5": ClassLabel.HEALTH,
    "LABEL_6": ClassLabel.POLITICS,
    "LABEL_7": ClassLabel.SCIENCE,
    "LABEL_8": ClassLabel.SOCIETY,
    "LABEL_9": ClassLabel.SPORTS,
    "LABEL_10": ClassLabel.TRAVEL,
}


class HFNewsClassificationModel(ClassificationModel):
    def __init__(self, model_path: Path, device: str = "cpu", max_length: int = 512) -> None:
        if not model_path.exists():
            raise FileNotFoundError(f"Classifier model path not found: {model_path}")
        self._model_path = model_path
        self._device = device
        self._max_length = max_length
        self._classifier = self._build_pipeline()

    def _resolve_device(self) -> int:
        normalized = self._device.strip().lower()
        if normalized == "cpu":
            return -1
        if normalized == "cuda":
            return 0
        if normalized.startswith("cuda:"):
            try:
                return int(normalized.split(":", 1)[1])
            except ValueError as exc:
                raise ValueError(f"Invalid classifier device: {self._device}") from exc
        raise ValueError(f"Unsupported classifier device: {self._device}")

    def _build_pipeline(self) -> Any:
        try:
            from transformers import pipeline
        except ImportError as exc:  # pragma: no cover - runtime dependency check
            raise RuntimeError("Transformers dependency is missing. Install requirements to run classification.") from exc

        return pipeline(
            "text-classification",
            model=str(self._model_path),
            tokenizer=str(self._model_path),
            device=self._resolve_device(),
        )

    def classify(self, text: str) -> ClassificationResult:
        if not text.strip():
            return ClassificationResult(
                class_label=ClassLabel.OTHER,
                class_confidence=0.0,
                model_version=MODEL_VERSION,
            )

        result = self._classifier(text, truncation=True, max_length=self._max_length)
        payload = result[0] if isinstance(result, list) else result
        raw_label = str(payload.get("label", ""))
        class_label = HF_LABEL_MAP.get(raw_label, ClassLabel.OTHER)
        confidence = float(payload.get("score", 0.0))
        return ClassificationResult(
            class_label=class_label,
            class_confidence=confidence,
            model_version=MODEL_VERSION,
        )
