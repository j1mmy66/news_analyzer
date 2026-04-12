from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import sqrt
from typing import Any

from news_analyzer.domain.models import DedupMetadataUpdate
from news_analyzer.nlp.dedup.interface import TextEmbeddingModel


@dataclass(frozen=True)
class _DedupItem:
    external_id: str
    published_at: datetime
    title: str
    text_for_embedding: str


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self._parent = list(range(size))
        self._rank = [0 for _ in range(size)]

    def find(self, node: int) -> int:
        parent = self._parent[node]
        if parent != node:
            self._parent[node] = self.find(parent)
        return self._parent[node]

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return

        if self._rank[root_left] < self._rank[root_right]:
            self._parent[root_left] = root_right
            return
        if self._rank[root_left] > self._rank[root_right]:
            self._parent[root_right] = root_left
            return

        self._parent[root_right] = root_left
        self._rank[root_left] += 1


class TransformerTextEmbeddingModel(TextEmbeddingModel):
    def __init__(self, model_name: str, device: str = "cpu", max_length: int = 512) -> None:
        self._max_length = max_length
        self._tokenizer, self._model, self._torch, self._device = self._build(model_name=model_name, device=device)

    @staticmethod
    def _build(model_name: str, device: str) -> tuple[Any, Any, Any, Any]:
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer
        except ImportError as exc:  # pragma: no cover - runtime dependency check
            raise RuntimeError("Transformers and torch dependencies are required for semantic deduplication.") from exc

        normalized = device.strip().lower()
        if normalized == "cpu":
            torch_device = torch.device("cpu")
        elif normalized.startswith("cuda"):
            torch_device = torch.device(normalized)
        else:
            raise ValueError(f"Unsupported dedup device: {device}")

        tokenizer = AutoTokenizer.from_pretrained(model_name)
        model = AutoModel.from_pretrained(model_name)
        model.to(torch_device)
        model.eval()
        return tokenizer, model, torch, torch_device

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []

        encoded = self._tokenizer(
            texts,
            padding=True,
            truncation=True,
            max_length=self._max_length,
            return_tensors="pt",
        )
        encoded = {key: value.to(self._device) for key, value in encoded.items()}

        with self._torch.no_grad():
            outputs = self._model(**encoded)
            token_embeddings = outputs.last_hidden_state
            attention_mask = encoded["attention_mask"].unsqueeze(-1).expand(token_embeddings.size()).float()
            summed = (token_embeddings * attention_mask).sum(dim=1)
            counts = attention_mask.sum(dim=1).clamp(min=1e-9)
            pooled = summed / counts
            normalized = self._torch.nn.functional.normalize(pooled, p=2, dim=1)

        return normalized.cpu().tolist()


class SemanticNewsDeduplicator:
    def __init__(
        self,
        embedding_model: TextEmbeddingModel,
        *,
        similarity_threshold: float = 0.90,
        window_hours: int = 3,
        text_chars: int = 1000,
    ) -> None:
        self._embedding_model = embedding_model
        self._similarity_threshold = similarity_threshold
        self._window_hours = window_hours
        self._text_chars = text_chars

    def deduplicate(self, items: list[dict[str, object]]) -> list[DedupMetadataUpdate]:
        prepared = self._prepare_items(items)
        if not prepared:
            return []

        prepared.sort(key=lambda value: (value.published_at, value.external_id))
        vectors = [self._normalize_vector(vector) for vector in self._embedding_model.embed([v.text_for_embedding for v in prepared])]

        disjoint_set = _DisjointSet(len(prepared))
        window = timedelta(hours=self._window_hours)
        for right in range(len(prepared)):
            left = right - 1
            while left >= 0 and prepared[right].published_at - prepared[left].published_at <= window:
                similarity = self._cosine_similarity(vectors[right], vectors[left])
                if similarity >= self._similarity_threshold:
                    disjoint_set.union(right, left)
                left -= 1

        clusters: dict[int, list[int]] = {}
        for index in range(len(prepared)):
            root = disjoint_set.find(index)
            clusters.setdefault(root, []).append(index)

        updates: list[DedupMetadataUpdate] = []
        for indices in clusters.values():
            canonical_index = max(indices, key=lambda idx: (prepared[idx].published_at, prepared[idx].external_id))
            canonical_id = prepared[canonical_index].external_id
            canonical_vector = vectors[canonical_index]
            for idx in indices:
                similarity_to_canonical = self._cosine_similarity(vectors[idx], canonical_vector)
                if idx == canonical_index:
                    similarity_to_canonical = 1.0
                updates.append(
                    DedupMetadataUpdate(
                        external_id=prepared[idx].external_id,
                        is_canonical=idx == canonical_index,
                        canonical_external_id=canonical_id,
                        similarity_to_canonical=similarity_to_canonical,
                    )
                )

        return updates

    def _prepare_items(self, items: list[dict[str, object]]) -> list[_DedupItem]:
        prepared: list[_DedupItem] = []
        for item in items:
            external_id = str(item.get("external_id") or "").strip()
            published_at = self._parse_datetime(item.get("published_at"))
            if not external_id or published_at is None:
                continue

            metadata = item.get("source_metadata")
            if not isinstance(metadata, dict):
                metadata = {}
            title = str(metadata.get("title") or "").strip()
            cleaned_text = str(item.get("cleaned_text") or "").strip()
            raw_text = str(item.get("raw_text") or "").strip()
            base_text = cleaned_text or raw_text
            composed = " ".join(part for part in [title, base_text[: self._text_chars]] if part).strip()
            if not composed:
                composed = external_id
            prepared.append(
                _DedupItem(
                    external_id=external_id,
                    published_at=published_at,
                    title=title,
                    text_for_embedding=composed,
                )
            )

        return prepared

    @staticmethod
    def _parse_datetime(value: object) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _normalize_vector(vector: list[float]) -> list[float]:
        norm = sqrt(sum(value * value for value in vector))
        if norm <= 0.0:
            return vector
        return [value / norm for value in vector]

    @staticmethod
    def _cosine_similarity(left: list[float], right: list[float]) -> float:
        if not left or not right or len(left) != len(right):
            return 0.0
        raw = sum(a * b for a, b in zip(left, right))
        if raw > 1.0:
            return 1.0
        if raw < -1.0:
            return -1.0
        return raw
