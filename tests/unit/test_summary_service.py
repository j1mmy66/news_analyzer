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


class _CountingClient:
    def __init__(self, response: str = "ok", should_fail: bool = False) -> None:
        self.response = response
        self.should_fail = should_fail
        self.calls = 0

    def summarize(self, prompt: str) -> str:
        self.calls += 1
        if self.should_fail:
            raise RuntimeError("boom")
        return self.response


def test_summary_service_item_empty_text_returns_failed() -> None:
    service = SummaryService(_CountingClient())
    result = service.summarize_item("   ")

    assert result.status == ProcessingStatus.FAILED
    assert result.error_code == "EMPTY_TEXT"


def test_summary_service_hour_empty_batch_returns_failed() -> None:
    service = SummaryService(_CountingClient())
    result = service.summarize_hour([])

    assert result.status == ProcessingStatus.FAILED
    assert result.error_code == "EMPTY_BATCH"


def test_summary_service_summarize_hour_uses_cache() -> None:
    client = _CountingClient(response="hour")
    service = SummaryService(client)

    first = service.summarize_hour(["a", "b"])
    second = service.summarize_hour(["a", "b"])

    assert first.status == ProcessingStatus.SUCCESS
    assert second.status == ProcessingStatus.SUCCESS
    assert second.summary == "hour"
    assert client.calls == 1


def test_summary_service_returns_error_code_on_item_failure() -> None:
    service = SummaryService(_CountingClient(should_fail=True))
    result = service.summarize_item("text")

    assert result.status == ProcessingStatus.FAILED
    assert result.error_code == "RuntimeError"


def test_summary_service_returns_error_code_on_hour_failure() -> None:
    service = SummaryService(_CountingClient(should_fail=True))
    result = service.summarize_hour(["one", "two"])

    assert result.status == ProcessingStatus.FAILED
    assert result.error_code == "RuntimeError"
