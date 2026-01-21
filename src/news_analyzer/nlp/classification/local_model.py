from __future__ import annotations

from news_analyzer.domain.enums import ClassLabel
from news_analyzer.domain.models import ClassificationResult
from news_analyzer.nlp.classification.interface import ClassificationModel


class KeywordClassificationModel(ClassificationModel):
    def classify(self, text: str) -> ClassificationResult:
        low = text.lower()
        if any(token in low for token in ["банк", "рынок", "инфляц", "эконом"]):
            label = ClassLabel.ECONOMY
            confidence = 0.7
        elif any(token in low for token in ["правитель", "президент", "закон", "госдума"]):
            label = ClassLabel.POLITICS
            confidence = 0.65
        elif any(token in low for token in ["технолог", "ai", "ии", "it"]):
            label = ClassLabel.TECHNOLOGY
            confidence = 0.6
        else:
            label = ClassLabel.OTHER
            confidence = 0.5

        return ClassificationResult(class_label=label, class_confidence=confidence, model_version="keyword-v1")
