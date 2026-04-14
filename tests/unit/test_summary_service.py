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
        self.prompts: list[str] = []

    def summarize(self, prompt: str) -> str:
        self.calls += 1
        self.prompts.append(prompt)
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


def test_summary_service_truncates_item_text_and_logs_metric(caplog) -> None:
    client = _CountingClient(response="ok")
    service = SummaryService(client, item_text_max_chars=5)
    caplog.set_level("INFO")

    result = service.summarize_item("abcdefgh")

    assert result.status == ProcessingStatus.SUCCESS
    assert client.calls == 1
    assert "Текст:\nabcde" in client.prompts[0]
    assert "text_truncated=True" in caplog.text
    assert "truncated_count=1" in caplog.text
    assert "limit_chars=5" in caplog.text


def test_summary_service_truncates_hourly_batch_latest_first(caplog) -> None:
    client = _CountingClient(response="ok")
    service = SummaryService(client, hourly_item_text_max_chars=3, hourly_total_text_max_chars=6, hourly_latest_first=True)
    caplog.set_level("INFO")

    result = service.summarize_hour(["1111", "2222", "3333"])

    assert result.status == ProcessingStatus.SUCCESS
    assert client.calls == 1
    prompt = client.prompts[0]
    assert "111" not in prompt
    assert "222" in prompt
    assert "333" in prompt
    assert "text_truncated=True" in caplog.text
    assert "truncated_count=3" in caplog.text
    assert "dropped_count=1" in caplog.text
