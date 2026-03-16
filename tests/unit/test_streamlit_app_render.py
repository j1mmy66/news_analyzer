from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone

from news_analyzer.apps.streamlit import app
from news_analyzer.apps.streamlit.view_models import HourlyDigestView, NewsCard, NewsCursor, NewsPage


class _FakeService:
    def __init__(self, pages: list[NewsPage], digest: HourlyDigestView | None) -> None:
        self._pages = pages
        self._digest = digest

    def latest_hourly_digest_for_last_hour(self) -> HourlyDigestView | None:
        return self._digest

    def latest_news_page(
        self,
        size: int = 50,
        cursor: NewsCursor | None = None,
        source: str | None = None,
        class_label: str | None = None,
    ) -> NewsPage:
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
