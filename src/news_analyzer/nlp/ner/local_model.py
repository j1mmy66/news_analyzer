from __future__ import annotations

import re

from news_analyzer.domain.models import Entity
from news_analyzer.nlp.ner.interface import NERModel


class RegexNERModel(NERModel):
    """A lightweight placeholder model to keep interfaces stable."""

    def extract(self, text: str) -> list[Entity]:
        entities: list[Entity] = []
        for match in re.finditer(r"\b[A-ZА-Я][a-zа-яA-ZА-Я\-]{2,}\b", text):
            entities.append(
                Entity(
                    text=match.group(0),
                    label="PROPER_NOUN",
                    start=match.start(),
                    end=match.end(),
                    confidence=0.4,
                    normalized=match.group(0).lower(),
                )
            )
        return entities
