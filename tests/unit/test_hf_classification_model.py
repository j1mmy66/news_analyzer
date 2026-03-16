from __future__ import annotations

from pathlib import Path

import pytest

from news_analyzer.domain.enums import ClassLabel
from news_analyzer.nlp.classification.local_model import HFNewsClassificationModel


def _prepare_model_dir(tmp_path: Path) -> Path:
    model_dir = tmp_path / "any-news-classifier"
    model_dir.mkdir(parents=True, exist_ok=True)
    return model_dir


@pytest.mark.parametrize(
    ("raw_label", "expected"),
    [
        ("LABEL_0", ClassLabel.CLIMATE),
        ("LABEL_1", ClassLabel.CONFLICTS),
        ("LABEL_2", ClassLabel.CULTURE),
        ("LABEL_3", ClassLabel.ECONOMY),
        ("LABEL_4", ClassLabel.GLOSS),
        ("LABEL_5", ClassLabel.HEALTH),
        ("LABEL_6", ClassLabel.POLITICS),
        ("LABEL_7", ClassLabel.SCIENCE),
        ("LABEL_8", ClassLabel.SOCIETY),
        ("LABEL_9", ClassLabel.SPORTS),
        ("LABEL_10", ClassLabel.TRAVEL),
    ],
)
def test_hf_classifier_maps_all_known_labels(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    raw_label: str,
    expected: ClassLabel,
) -> None:
    model_dir = _prepare_model_dir(tmp_path)

    def _fake_pipeline(_text: str, truncation: bool, max_length: int) -> list[dict[str, object]]:
        assert truncation is True
        assert max_length == 256
        return [{"label": raw_label, "score": 0.91}]

    monkeypatch.setattr(HFNewsClassificationModel, "_build_pipeline", lambda self: _fake_pipeline)
    model = HFNewsClassificationModel(model_path=model_dir, device="cpu", max_length=256)

    result = model.classify("Новости науки")
    assert result.class_label == expected
    assert result.class_confidence == 0.91
    assert result.model_version == "hf-any-news-v1"


def test_hf_classifier_handles_unknown_label(tmp_path: Path, monkeypatch) -> None:
    model_dir = _prepare_model_dir(tmp_path)
    monkeypatch.setattr(
        HFNewsClassificationModel,
        "_build_pipeline",
        lambda self: (lambda _text, truncation, max_length: [{"label": "UNKNOWN", "score": 0.42}]),
    )
    model = HFNewsClassificationModel(model_path=model_dir)

    result = model.classify("Случайный текст")
    assert result.class_label == ClassLabel.OTHER
    assert result.class_confidence == 0.42


def test_hf_classifier_handles_empty_text(tmp_path: Path, monkeypatch) -> None:
    model_dir = _prepare_model_dir(tmp_path)
    monkeypatch.setattr(HFNewsClassificationModel, "_build_pipeline", lambda self: lambda *_args, **_kwargs: [])
    model = HFNewsClassificationModel(model_path=model_dir)

    result = model.classify("   ")
    assert result.class_label == ClassLabel.OTHER
    assert result.class_confidence == 0.0
