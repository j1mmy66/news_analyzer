from __future__ import annotations

from pathlib import Path

from news_analyzer.domain.enums import ClassLabel
from news_analyzer.domain.models import ClassificationResult, Entity
from news_analyzer.pipeline.enrich import ner_job
from news_analyzer.settings.app_settings import AppSettings


class _RepoStub:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = items
        self.calls: list[tuple[str, list[Entity], ClassificationResult]] = []

    def get_news_for_last_hour(self, limit: int = 300) -> list[dict[str, object]]:
        return self._items[:limit]

    def set_enrichment(self, external_id: str, entities: list[Entity], classification: ClassificationResult) -> None:
        self.calls.append((external_id, entities, classification))


class _ClassifierStub:
    def classify(self, _text: str) -> ClassificationResult:
        return ClassificationResult(class_label=ClassLabel.ECONOMY, class_confidence=0.7, model_version="stub")


def _settings(tmp_path: Path) -> AppSettings:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    model_path.write_text("model", encoding="utf-8")
    navec_path.write_text("model", encoding="utf-8")
    return AppSettings(
        opensearch_hosts=["http://localhost:9200"],
        ner_slovnet_model_path=model_path,
        ner_navec_path=navec_path,
        ner_max_retries=2,
        ner_retry_backoff_seconds=0.01,
        ner_retry_backoff_cap_seconds=0.02,
    )


def _patch_common(
    monkeypatch,
    repo: _RepoStub,
    settings: AppSettings,
    model,
) -> None:
    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_job, "NewsRepository", lambda _client, _index: repo)
    monkeypatch.setattr(ner_job, "KeywordClassificationModel", lambda: _ClassifierStub())
    monkeypatch.setattr(ner_job, "NatashaSlovnetNERModel", lambda **kwargs: model)


def test_run_ner_job_success_path(tmp_path: Path, monkeypatch) -> None:
    repo = _RepoStub([{"external_id": "n1", "cleaned_text": "Иван Иванов"}])
    settings = _settings(tmp_path)

    class _Model:
        def extract(self, _text: str) -> list[Entity]:
            return [Entity(text="Иван Иванов", label="PER", start=0, end=11, confidence=0.8, normalized="иван иванов")]

    _patch_common(monkeypatch, repo, settings, _Model())
    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert len(repo.calls) == 1
    assert repo.calls[0][1][0].label == "PER"


def test_run_ner_job_retries_then_succeeds(tmp_path: Path, monkeypatch) -> None:
    repo = _RepoStub([{"external_id": "n2", "cleaned_text": "Рынок растет"}])
    settings = _settings(tmp_path)
    delays: list[float] = []

    class _Model:
        def __init__(self) -> None:
            self.calls = 0

        def extract(self, _text: str) -> list[Entity]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("temporary")
            return [Entity(text="Рынок", label="ORG", start=0, end=5, confidence=0.9, normalized="рынок")]

    model = _Model()
    _patch_common(monkeypatch, repo, settings, model)
    monkeypatch.setattr(ner_job.time, "sleep", lambda seconds: delays.append(seconds))

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert model.calls == 2
    assert delays == [0.01]
    assert len(repo.calls[0][1]) == 1


def test_run_ner_job_persists_empty_entities_after_ner_failures(tmp_path: Path, monkeypatch) -> None:
    repo = _RepoStub([{"external_id": "n3", "cleaned_text": "Госдума"}])
    settings = _settings(tmp_path)
    delays: list[float] = []

    class _Model:
        def extract(self, _text: str) -> list[Entity]:
            raise RuntimeError("permanent")

    _patch_common(monkeypatch, repo, settings, _Model())
    monkeypatch.setattr(ner_job.time, "sleep", lambda seconds: delays.append(seconds))

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert len(repo.calls) == 1
    assert repo.calls[0][1] == []
    assert delays == [0.01, 0.02]
