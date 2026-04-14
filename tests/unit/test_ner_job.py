from __future__ import annotations

from pathlib import Path

from news_analyzer.domain.enums import ClassLabel, ProcessingStatus
from news_analyzer.domain.models import ClassificationResult, Entity
from news_analyzer.pipeline.enrich import ner_job
from news_analyzer.settings.app_settings import AppSettings


class _RepoStub:
    def __init__(self, items: list[dict[str, object]]) -> None:
        self._items = items
        self.calls: list[dict[str, object]] = []

    def get_recent_news_without_enrichment(self, limit: int = 300, hours: int = 24) -> list[dict[str, object]]:
        assert hours == 24
        return self._items[:limit]

    def set_enrichment(
        self,
        external_id: str,
        entities: list[Entity],
        classification: ClassificationResult,
        *,
        enrichment_status: ProcessingStatus = ProcessingStatus.SUCCESS,
        enrichment_error_code: str | None = None,
    ) -> None:
        self.calls.append(
            {
                "external_id": external_id,
                "entities": entities,
                "classification": classification,
                "enrichment_status": enrichment_status,
                "enrichment_error_code": enrichment_error_code,
            }
        )


class _ClassifierStub:
    def classify(self, _text: str) -> ClassificationResult:
        return ClassificationResult(class_label=ClassLabel.ECONOMY, class_confidence=0.7, model_version="stub")


def _settings(tmp_path: Path) -> AppSettings:
    model_path = tmp_path / "slovnet.tar"
    navec_path = tmp_path / "navec.tar"
    cls_path = tmp_path / "any-news-classifier"
    cls_path.mkdir(parents=True, exist_ok=True)
    model_path.write_text("model", encoding="utf-8")
    navec_path.write_text("model", encoding="utf-8")
    return AppSettings(
        opensearch_hosts=["http://localhost:9200"],
        classifier_model_path=cls_path,
        classifier_device="cpu",
        classifier_max_length=64,
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
    monkeypatch.setattr(ner_job, "HFNewsClassificationModel", lambda **kwargs: _ClassifierStub())
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
    assert repo.calls[0]["entities"][0].label == "PER"
    assert repo.calls[0]["enrichment_status"] == ProcessingStatus.SUCCESS
    assert repo.calls[0]["enrichment_error_code"] is None


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
    assert len(repo.calls[0]["entities"]) == 1
    assert repo.calls[0]["enrichment_status"] == ProcessingStatus.SUCCESS
    assert repo.calls[0]["enrichment_error_code"] is None


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
    assert repo.calls[0]["entities"] == []
    assert repo.calls[0]["enrichment_status"] == ProcessingStatus.FAILED
    assert repo.calls[0]["enrichment_error_code"] == "NER_RuntimeError"
    assert delays == [0.01, 0.02]


def test_run_ner_job_fallbacks_to_other_on_classification_failure(tmp_path: Path, monkeypatch) -> None:
    repo = _RepoStub([{"external_id": "n4", "cleaned_text": "Текст новости"}])
    settings = _settings(tmp_path)

    class _NERModel:
        def extract(self, _text: str) -> list[Entity]:
            return [Entity(text="Москва", label="LOC", start=0, end=6, confidence=0.8, normalized="москва")]

    class _FailingClassifier:
        def classify(self, _text: str) -> ClassificationResult:
            raise RuntimeError("boom")

    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_job, "NewsRepository", lambda _client, _index: repo)
    monkeypatch.setattr(ner_job, "HFNewsClassificationModel", lambda **kwargs: _FailingClassifier())
    monkeypatch.setattr(ner_job, "NatashaSlovnetNERModel", lambda **kwargs: _NERModel())

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert len(repo.calls) == 1
    assert repo.calls[0]["classification"].class_label == ClassLabel.OTHER
    assert repo.calls[0]["classification"].class_confidence == 0.0
    assert repo.calls[0]["enrichment_status"] == ProcessingStatus.FAILED
    assert repo.calls[0]["enrichment_error_code"] == "CLASSIFICATION_RuntimeError"


def test_run_ner_job_marks_failed_when_ner_and_classification_fail(tmp_path: Path, monkeypatch) -> None:
    repo = _RepoStub([{"external_id": "n4b", "cleaned_text": "Текст новости"}])
    settings = _settings(tmp_path)

    class _FailingNERModel:
        def extract(self, _text: str) -> list[Entity]:
            raise RuntimeError("ner-fail")

    class _FailingClassifier:
        def classify(self, _text: str) -> ClassificationResult:
            raise RuntimeError("clf-fail")

    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_job, "NewsRepository", lambda _client, _index: repo)
    monkeypatch.setattr(ner_job, "HFNewsClassificationModel", lambda **kwargs: _FailingClassifier())
    monkeypatch.setattr(ner_job, "NatashaSlovnetNERModel", lambda **kwargs: _FailingNERModel())

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert len(repo.calls) == 1
    assert repo.calls[0]["entities"] == []
    assert repo.calls[0]["classification"].class_label == ClassLabel.OTHER
    assert repo.calls[0]["enrichment_status"] == ProcessingStatus.FAILED
    assert repo.calls[0]["enrichment_error_code"] == "NER_AND_CLASSIFICATION_FAILED"


def test_run_ner_job_trims_template_phrase_for_ner_and_classification(tmp_path: Path, monkeypatch) -> None:
    source_text = "До маркера. Самые важные новости: хвост который нужно убрать."
    repo = _RepoStub([{"external_id": "n5", "cleaned_text": source_text}])
    settings = _settings(tmp_path)
    seen: dict[str, str] = {}

    class _NERModel:
        def extract(self, text: str) -> list[Entity]:
            seen["ner"] = text
            return []

    class _Classifier:
        def classify(self, text: str) -> ClassificationResult:
            seen["clf"] = text
            return ClassificationResult(class_label=ClassLabel.ECONOMY, class_confidence=0.7, model_version="stub")

    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_job, "NewsRepository", lambda _client, _index: repo)
    monkeypatch.setattr(ner_job, "HFNewsClassificationModel", lambda **kwargs: _Classifier())
    monkeypatch.setattr(ner_job, "NatashaSlovnetNERModel", lambda **kwargs: _NERModel())

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert seen["ner"] == "До маркера."
    assert seen["clf"] == "До маркера."


def test_run_ner_job_trims_template_phrase_case_insensitive(tmp_path: Path, monkeypatch) -> None:
    source_text = "Вступление самые важные новости и дальше шаблон."
    repo = _RepoStub([{"external_id": "n6", "cleaned_text": source_text}])
    settings = _settings(tmp_path)
    seen: dict[str, str] = {}

    class _NERModel:
        def extract(self, text: str) -> list[Entity]:
            seen["ner"] = text
            return []

    class _Classifier:
        def classify(self, text: str) -> ClassificationResult:
            seen["clf"] = text
            return ClassificationResult(class_label=ClassLabel.ECONOMY, class_confidence=0.7, model_version="stub")

    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_job, "NewsRepository", lambda _client, _index: repo)
    monkeypatch.setattr(ner_job, "HFNewsClassificationModel", lambda **kwargs: _Classifier())
    monkeypatch.setattr(ner_job, "NatashaSlovnetNERModel", lambda **kwargs: _NERModel())

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert seen["ner"] == "Вступление"
    assert seen["clf"] == "Вступление"


def test_run_ner_job_keeps_original_text_when_phrase_missing(tmp_path: Path, monkeypatch) -> None:
    source_text = "Обычный текст новости без служебного хвоста."
    repo = _RepoStub([{"external_id": "n7", "cleaned_text": source_text}])
    settings = _settings(tmp_path)
    seen: dict[str, str] = {}

    class _NERModel:
        def extract(self, text: str) -> list[Entity]:
            seen["ner"] = text
            return []

    class _Classifier:
        def classify(self, text: str) -> ClassificationResult:
            seen["clf"] = text
            return ClassificationResult(class_label=ClassLabel.ECONOMY, class_confidence=0.7, model_version="stub")

    monkeypatch.setattr(ner_job.AppSettings, "from_env", classmethod(lambda cls: settings))
    monkeypatch.setattr(ner_job, "build_client", lambda _config: object())
    monkeypatch.setattr(ner_job, "NewsRepository", lambda _client, _index: repo)
    monkeypatch.setattr(ner_job, "HFNewsClassificationModel", lambda **kwargs: _Classifier())
    monkeypatch.setattr(ner_job, "NatashaSlovnetNERModel", lambda **kwargs: _NERModel())

    processed = ner_job.run_ner_job(limit=10)

    assert processed == 1
    assert seen["ner"] == source_text
    assert seen["clf"] == source_text


def test_extract_with_retry_returns_empty_when_no_attempts_configured() -> None:
    class _Model:
        def extract(self, text: str):
            raise AssertionError("extract should not be called")

    entities = ner_job._extract_with_retry(
        ner_model=_Model(),  # type: ignore[arg-type]
        text="text",
        max_retries=-1,
        backoff_seconds=0.1,
        backoff_cap_seconds=1.0,
    )
    assert entities == []


def test_run_ner_job_logs_persistence_error_and_continues(tmp_path: Path, monkeypatch) -> None:
    class _FailingRepo(_RepoStub):
        def set_enrichment(
            self,
            external_id: str,
            entities: list[Entity],
            classification: ClassificationResult,
            *,
            enrichment_status: ProcessingStatus = ProcessingStatus.SUCCESS,
            enrichment_error_code: str | None = None,
        ) -> None:
            raise RuntimeError("persist-failed")

    repo = _FailingRepo([{"external_id": "n8", "cleaned_text": "Текст"}])
    settings = _settings(tmp_path)

    class _NERModel:
        def extract(self, _text: str) -> list[Entity]:
            return []

    _patch_common(monkeypatch, repo, settings, _NERModel())
    processed = ner_job.run_ner_job(limit=10)

    assert processed == 0
