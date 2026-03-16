from news_analyzer.domain.enums import ProcessingStatus
from news_analyzer.summarization.gigachat.client import GigaChatError
from news_analyzer.summarization.service import SummaryService


class _OkClient:
    def summarize(self, prompt: str) -> str:
        return "short summary"


class _FailClient:
    def summarize(self, prompt: str) -> str:
        raise GigaChatError("down")


def test_summary_service_graceful_degradation() -> None:
    service = SummaryService(_FailClient())
    result = service.summarize_item("some text")

    assert result.status == ProcessingStatus.FAILED
    assert result.summary is None


def test_summary_service_cache_and_success() -> None:
    service = SummaryService(_OkClient())
    first = service.summarize_item("hello world")
    second = service.summarize_item("hello world")

    assert first.status == ProcessingStatus.SUCCESS
    assert second.status == ProcessingStatus.SUCCESS
    assert second.summary == "short summary"
