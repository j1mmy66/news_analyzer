from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import runpy
import sys
import types

from news_analyzer.apps.streamlit import app
from news_analyzer.apps.streamlit.view_models import HourlyDigestView, NewsCard, NewsCursor, NewsPage


class _FakeService:
    def __init__(self, pages: list[NewsPage], digest: HourlyDigestView | None) -> None:
        self._pages = pages
        self._digest = digest
        self.calls = 0

    def latest_hourly_digest_for_last_hour(self) -> HourlyDigestView | None:
        return self._digest

    def latest_news_page(
        self,
        size: int = 50,
        cursor: NewsCursor | None = None,
        source: str | None = None,
        class_label: str | None = None,
    ) -> NewsPage:
        self.calls += 1
        return self._pages.pop(0)


class _FakeStreamlit:
    def __init__(self, *, button_value: bool) -> None:
        self.session_state: dict[str, object] = {}
        self.button_value = button_value
        self.subheaders: list[str] = []
        self.writes: list[str] = []
        self.expanders: list[str] = []
        self.rerun_called = False

    def set_page_config(self, **kwargs) -> None:
        return None

    def title(self, text: str) -> None:
        self.writes.append(text)

    def subheader(self, text: str) -> None:
        self.subheaders.append(text)

    def info(self, text: str) -> None:
        self.writes.append(text)

    def error(self, text: str) -> None:
        self.writes.append(text)

    def write(self, text: object) -> None:
        self.writes.append(str(text))

    def caption(self, text: str) -> None:
        self.writes.append(text)

    def divider(self) -> None:
        return None

    def selectbox(self, label: str, options: list[str], index: int = 0) -> str:
        return options[index]

    def text_input(self, label: str, value: str = "") -> str:
        return value

    def markdown(self, text: str) -> None:
        self.writes.append(text)

    @contextmanager
    def expander(self, label: str):
        self.expanders.append(label)
        yield None

    def button(self, label: str) -> bool:
        return self.button_value

    def rerun(self) -> None:
        self.rerun_called = True


def test_render_app_renders_hourly_digest_and_news_card(monkeypatch) -> None:
    digest = HourlyDigestView(
        digest_id="digest-1",
        window_start=datetime(2026, 3, 17, 7, 0, tzinfo=timezone.utc),
        window_end=datetime(2026, 3, 17, 7, 59, tzinfo=timezone.utc),
        summary="hour summary",
        news_count=1,
    )
    first_page = NewsPage(
        items=[
            NewsCard(
                external_id="id-1",
                title="Title 1",
                summary="Summary 1",
                class_label="economy",
                published_at=datetime(2026, 3, 17, 7, 30, tzinfo=timezone.utc),
                source_type="rbc",
                raw_text="Full raw text",
                url="https://example.com/id-1",
                authors="Author 1",
                section="economics",
            )
        ],
        next_cursor=NewsCursor("2026-03-17T07:30:00+00:00", "id-1"),
        has_more=False,
    )
    fake_service = _FakeService(pages=[first_page], digest=digest)
    fake_st = _FakeStreamlit(button_value=False)

    monkeypatch.setattr(app, "_query_service", lambda: fake_service)
    monkeypatch.setattr(app, "st", fake_st)

    app.render_app()

    assert "Саммари за последний час" in fake_st.subheaders
    assert "Лента новостей" in fake_st.subheaders
    assert "Показать новость целиком" in fake_st.expanders
    assert any("Class: economy" in line for line in fake_st.writes)
    assert any("Summary 1" in line for line in fake_st.writes)
    assert any("Full raw text" in line for line in fake_st.writes)


def test_render_app_load_more_updates_state(monkeypatch) -> None:
    first_page = NewsPage(
        items=[
            NewsCard(
                external_id="id-1",
                title="Title 1",
                summary=None,
                class_label="economy",
                published_at=datetime(2026, 3, 17, 7, 30, tzinfo=timezone.utc),
                source_type="rbc",
                raw_text="Raw 1",
                url=None,
                authors="",
                section=None,
            )
        ],
        next_cursor=NewsCursor("2026-03-17T07:30:00+00:00", "id-1"),
        has_more=True,
    )
    second_page = NewsPage(
        items=[
            NewsCard(
                external_id="id-2",
                title="Title 2",
                summary="Summary 2",
                class_label="politics",
                published_at=datetime(2026, 3, 17, 7, 20, tzinfo=timezone.utc),
                source_type="rbc",
                raw_text="Raw 2",
                url=None,
                authors="",
                section=None,
            )
        ],
        next_cursor=None,
        has_more=False,
    )
    fake_service = _FakeService(pages=[first_page, second_page], digest=None)
    fake_st = _FakeStreamlit(button_value=True)

    monkeypatch.setattr(app, "_query_service", lambda: fake_service)
    monkeypatch.setattr(app, "st", fake_st)

    app.render_app()

    items = fake_st.session_state[app.STATE_ITEMS_KEY]
    assert isinstance(items, list)
    assert len(items) == 2
    assert fake_st.session_state[app.STATE_HAS_MORE_KEY] is False
    assert fake_st.rerun_called is True


def test_ensure_and_reset_feed_state(monkeypatch) -> None:
    fake_st = _FakeStreamlit(button_value=False)
    monkeypatch.setattr(app, "st", fake_st)

    app._ensure_feed_state()
    assert fake_st.session_state[app.STATE_ITEMS_KEY] == []
    assert fake_st.session_state[app.STATE_HAS_MORE_KEY] is True

    app._reset_feed_state(source="rbc", class_label="economy")
    assert fake_st.session_state[app.STATE_SOURCE_KEY] == "rbc"
    assert fake_st.session_state[app.STATE_CLASS_KEY] == "economy"
    assert fake_st.session_state[app.STATE_CURSOR_KEY] is None


def test_load_more_news_skips_when_has_more_is_false(monkeypatch) -> None:
    fake_st = _FakeStreamlit(button_value=False)
    fake_st.session_state[app.STATE_HAS_MORE_KEY] = False
    fake_service = _FakeService(pages=[], digest=None)
    monkeypatch.setattr(app, "st", fake_st)

    app._load_more_news(fake_service, source="", class_label="")
    assert fake_service.calls == 0


def test_render_app_shows_not_found_when_page_is_empty(monkeypatch) -> None:
    empty_page = NewsPage(items=[], next_cursor=None, has_more=False)
    fake_service = _FakeService(pages=[empty_page], digest=None)
    fake_st = _FakeStreamlit(button_value=False)

    monkeypatch.setattr(app, "_query_service", lambda: fake_service)
    monkeypatch.setattr(app, "st", fake_st)

    app.render_app()

    assert any("Новости не найдены." in line for line in fake_st.writes)


def test_app_query_service_builds_client_from_settings(monkeypatch) -> None:
    class _Settings:
        opensearch_hosts = ["http://localhost:9200"]
        opensearch_news_index = "news_items"
        opensearch_digests_index = "hourly_digests"
        opensearch_username = "u"
        opensearch_password = "p"
        opensearch_use_ssl = True
        opensearch_verify_certs = True

    def _fake_build_client(config):
        return "client-object"

    class _FakeQueryService:
        def __init__(self, client: object, news_index: str, digest_index: str) -> None:
            self.client = client
            self.news_index = news_index
            self.digest_index = digest_index

    monkeypatch.setattr(app.AppSettings, "from_env", classmethod(lambda cls: _Settings()))
    monkeypatch.setattr(app, "build_client", _fake_build_client)
    monkeypatch.setattr(app, "StreamlitQueryService", _FakeQueryService)

    service = app._query_service.__wrapped__()

    assert isinstance(service, _FakeQueryService)
    assert service.client == "client-object"
    assert service.news_index == "news_items"
    assert service.digest_index == "hourly_digests"


def test_app_format_dt_handles_none() -> None:
    assert app._format_dt(None) == "n/a"


def test_render_app_resets_feed_state_when_filters_change(monkeypatch) -> None:
    fake_st = _FakeStreamlit(button_value=False)
    fake_st.session_state = {
        app.STATE_SOURCE_KEY: "",
        app.STATE_CLASS_KEY: "",
        app.STATE_ITEMS_KEY: ["old-item"],
        app.STATE_CURSOR_KEY: "old-cursor",
        app.STATE_HAS_MORE_KEY: False,
    }
    fake_service = _FakeService(
        pages=[NewsPage(items=[], next_cursor=None, has_more=False)],
        digest=None,
    )

    monkeypatch.setattr(app, "_query_service", lambda: fake_service)
    monkeypatch.setattr(app, "st", fake_st)
    monkeypatch.setattr(fake_st, "selectbox", lambda *args, **kwargs: "rbc")
    monkeypatch.setattr(fake_st, "text_input", lambda *args, **kwargs: "economy")

    app.render_app()

    assert fake_st.session_state[app.STATE_SOURCE_KEY] == "rbc"
    assert fake_st.session_state[app.STATE_CLASS_KEY] == "economy"


def test_app_main_guard_executes_render_app(monkeypatch) -> None:
    fake_st_module = types.ModuleType("streamlit")
    fake_st_module.session_state = {}
    fake_st_module.cache_resource = lambda fn: fn
    fake_st_module.set_page_config = lambda **kwargs: None
    fake_st_module.title = lambda text: None
    fake_st_module.subheader = lambda text: None
    fake_st_module.info = lambda text: None
    fake_st_module.write = lambda text: None
    fake_st_module.caption = lambda text: None
    fake_st_module.divider = lambda: None
    fake_st_module.selectbox = lambda *args, **kwargs: ""
    fake_st_module.text_input = lambda *args, **kwargs: ""
    fake_st_module.markdown = lambda text: None
    fake_st_module.button = lambda label: False
    fake_st_module.rerun = lambda: None

    @contextmanager
    def _expander(_label: str):
        yield None

    fake_st_module.expander = _expander

    fake_settings_module = types.ModuleType("news_analyzer.settings.app_settings")

    class _FakeSettings:
        opensearch_hosts = ["http://localhost:9200"]
        opensearch_news_index = "news_items"
        opensearch_digests_index = "hourly_digests"
        opensearch_username = None
        opensearch_password = None
        opensearch_use_ssl = False
        opensearch_verify_certs = False

    class _AppSettings:
        @classmethod
        def from_env(cls):
            return _FakeSettings()

    fake_settings_module.AppSettings = _AppSettings

    fake_client_module = types.ModuleType("news_analyzer.storage.opensearch.client")

    class _OpenSearchConfig:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    fake_client_module.OpenSearchConfig = _OpenSearchConfig
    fake_client_module.build_client = lambda config: object()

    fake_query_module = types.ModuleType("news_analyzer.apps.streamlit.query_service")

    class _Service:
        def __init__(self, client: object, news_index: str, digest_index: str) -> None:
            return None

        def latest_hourly_digest_for_last_hour(self):
            return None

        def latest_news_page(self, **kwargs):
            return types.SimpleNamespace(items=[], next_cursor=None, has_more=False)

    fake_query_module.StreamlitQueryService = _Service

    monkeypatch.setitem(__import__("sys").modules, "streamlit", fake_st_module)
    monkeypatch.setitem(__import__("sys").modules, "news_analyzer.settings.app_settings", fake_settings_module)
    monkeypatch.setitem(__import__("sys").modules, "news_analyzer.storage.opensearch.client", fake_client_module)
    monkeypatch.setitem(__import__("sys").modules, "news_analyzer.apps.streamlit.query_service", fake_query_module)
    monkeypatch.delitem(sys.modules, "news_analyzer.apps.streamlit.app", raising=False)

    runpy.run_module("news_analyzer.apps.streamlit.app", run_name="__main__")


def test_render_app_includes_lenta_source_in_selectbox(monkeypatch) -> None:
    fake_st = _FakeStreamlit(button_value=False)
    fake_service = _FakeService(pages=[NewsPage(items=[], next_cursor=None, has_more=False)], digest=None)
    seen: dict[str, object] = {}

    def _selectbox(label: str, options: list[str], index: int = 0) -> str:
        seen["options"] = options
        return options[index]

    monkeypatch.setattr(app, "_query_service", lambda: fake_service)
    monkeypatch.setattr(app, "st", fake_st)
    monkeypatch.setattr(fake_st, "selectbox", _selectbox)

    app.render_app()

    assert seen["options"] == ["", "rbc", "lenta"]


def test_render_app_handles_digest_backend_error(monkeypatch) -> None:
    class _FailService:
        def latest_hourly_digest_for_last_hour(self):
            raise RuntimeError("opensearch down")

        def latest_news_page(self, **kwargs):
            return NewsPage(items=[], next_cursor=None, has_more=False)

    fake_st = _FakeStreamlit(button_value=False)
    monkeypatch.setattr(app, "_query_service", lambda: _FailService())
    monkeypatch.setattr(app, "st", fake_st)

    app.render_app()

    assert any("OpenSearch недоступен" in line for line in fake_st.writes)
