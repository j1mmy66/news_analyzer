from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from news_analyzer.apps.streamlit.pages import feed, hourly_digest
from news_analyzer.apps.streamlit.view_models import HourlyDigestView, NewsPage


class _FakeStreamlit:
    def __init__(self) -> None:
        self.subheaders: list[str] = []
        self.writes: list[str] = []
        self.markdowns: list[str] = []
        self.infos: list[str] = []
        self.dividers = 0
        self.selected = ""
        self.input_value = ""

    def subheader(self, text: str) -> None:
        self.subheaders.append(text)

    def selectbox(self, label: str, options: list[str], index: int = 0):
        return self.selected if self.selected in options else options[index]

    def text_input(self, label: str):
        return self.input_value

    def markdown(self, text: str) -> None:
        self.markdowns.append(text)

    def write(self, value: object) -> None:
        self.writes.append(str(value))

    def divider(self) -> None:
        self.dividers += 1

    def info(self, text: str) -> None:
        self.infos.append(text)

    def error(self, text: str) -> None:
        self.infos.append(text)


class _FakeFeedService:
    def __init__(self, page: NewsPage) -> None:
        self.page = page
        self.calls: list[dict[str, object]] = []

    def latest_news_page(self, source=None, class_label=None):
        self.calls.append({"source": source, "class_label": class_label})
        return self.page


class _FakeDigestService:
    def __init__(self, digest: HourlyDigestView | None) -> None:
        self.digest = digest

    def latest_hourly_digest_for_last_hour(self) -> HourlyDigestView | None:
        return self.digest


def test_feed_format_dt_handles_none_and_datetime() -> None:
    assert feed._format_dt(None) == "n/a"

    value = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)
    expected = value.astimezone(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S %Z")
    assert feed._format_dt(value) == expected


def test_render_feed_renders_items_and_applies_filters(monkeypatch) -> None:
    fake_st = _FakeStreamlit()
    fake_st.selected = "rbc"
    fake_st.input_value = "economy"

    page = NewsPage(items=[], next_cursor=None, has_more=False)
    page.items.append(
        type("_Item", (), {
            "title": "Title 1",
            "source_type": "rbc",
            "class_label": "economy",
            "published_at": datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc),
            "summary": None,
            "url": "https://example.com/item-1",
        })()
    )
    service = _FakeFeedService(page=page)

    monkeypatch.setattr(feed, "st", fake_st)
    monkeypatch.setattr(feed, "_query_service", lambda: service)

    feed.render_feed()

    assert service.calls == [{"source": "rbc", "class_label": "economy"}]
    assert "News Feed" in fake_st.subheaders
    assert any("Source: rbc | Class: economy" in line for line in fake_st.writes)
    assert any("Summary is pending" in line for line in fake_st.writes)
    assert any("[Open source](https://example.com/item-1)" in line for line in fake_st.markdowns)
    assert fake_st.dividers == 1


def test_hourly_digest_format_dt_handles_none_and_datetime() -> None:
    assert hourly_digest._format_dt(None) == "n/a"

    value = datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc)
    expected = value.astimezone(ZoneInfo("Europe/Moscow")).strftime("%Y-%m-%d %H:%M:%S %Z")
    assert hourly_digest._format_dt(value) == expected


def test_render_hourly_digest_when_digest_is_missing(monkeypatch) -> None:
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(hourly_digest, "st", fake_st)
    monkeypatch.setattr(hourly_digest, "_query_service", lambda: _FakeDigestService(digest=None))

    hourly_digest.render_hourly_digest()

    assert "Hourly Digest" in fake_st.subheaders
    assert fake_st.infos == ["No hourly digest available for the last hour"]


def test_render_hourly_digest_when_digest_exists(monkeypatch) -> None:
    digest = HourlyDigestView(
        digest_id="digest-1",
        window_start=datetime(2026, 3, 17, 7, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 3, 17, 8, 0, tzinfo=timezone.utc),
        summary="hour summary",
        news_count=3,
    )
    fake_st = _FakeStreamlit()

    monkeypatch.setattr(hourly_digest, "st", fake_st)
    monkeypatch.setattr(hourly_digest, "_query_service", lambda: _FakeDigestService(digest=digest))

    hourly_digest.render_hourly_digest()

    assert "Hourly Digest" in fake_st.subheaders
    assert any("Window:" in line for line in fake_st.writes)
    assert any("hour summary" in line for line in fake_st.writes)
    assert any("Items in digest: 3" in line for line in fake_st.writes)


def test_feed_query_service_builds_client_from_settings(monkeypatch) -> None:
    class _Settings:
        opensearch_hosts = ["http://localhost:9200"]
        opensearch_news_index = "news_items"
        opensearch_digests_index = "hourly_digests"
        opensearch_username = "u"
        opensearch_password = "p"
        opensearch_use_ssl = True
        opensearch_verify_certs = True

    captured: dict[str, object] = {}

    def _fake_build_client(config):
        captured["config"] = config
        return "client-object"

    class _FakeQueryService:
        def __init__(self, client: object, news_index: str, digest_index: str) -> None:
            self.client = client
            self.news_index = news_index
            self.digest_index = digest_index

    monkeypatch.setattr(feed.AppSettings, "from_env", classmethod(lambda cls: _Settings()))
    monkeypatch.setattr(feed, "build_client", _fake_build_client)
    monkeypatch.setattr(feed, "StreamlitQueryService", _FakeQueryService)

    service = feed._query_service.__wrapped__()

    assert isinstance(service, _FakeQueryService)
    assert service.client == "client-object"
    assert service.news_index == "news_items"
    assert service.digest_index == "hourly_digests"


def test_render_feed_includes_lenta_source_in_selectbox(monkeypatch) -> None:
    fake_st = _FakeStreamlit()
    seen: dict[str, object] = {}

    def _selectbox(label: str, options: list[str], index: int = 0):
        seen["options"] = options
        return options[index]

    fake_st.selectbox = _selectbox  # type: ignore[method-assign]
    monkeypatch.setattr(feed, "st", fake_st)
    monkeypatch.setattr(feed, "_query_service", lambda: _FakeFeedService(page=NewsPage(items=[], next_cursor=None, has_more=False)))

    feed.render_feed()

    assert seen["options"] == ["", "rbc", "lenta"]


def test_render_feed_handles_backend_error(monkeypatch) -> None:
    class _FailService:
        def latest_news_page(self, source=None, class_label=None):
            raise RuntimeError("opensearch down")

    fake_st = _FakeStreamlit()
    monkeypatch.setattr(feed, "st", fake_st)
    monkeypatch.setattr(feed, "_query_service", lambda: _FailService())

    feed.render_feed()

    assert any("OpenSearch is unavailable" in line for line in fake_st.infos)
