from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st

from news_analyzer.apps.streamlit.query_service import StreamlitQueryService
from news_analyzer.apps.streamlit.view_models import NewsCard
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client

DEFAULT_BATCH_SIZE = 50
MOSCOW_TZ = ZoneInfo("Europe/Moscow")

STATE_ITEMS_KEY = "feed_items"
STATE_CURSOR_KEY = "feed_cursor"
STATE_HAS_MORE_KEY = "feed_has_more"
STATE_SOURCE_KEY = "feed_source_filter"
STATE_CLASS_KEY = "feed_class_filter"


@st.cache_resource
def _query_service() -> StreamlitQueryService:
    settings = AppSettings.from_env()
    client = build_client(
        OpenSearchConfig(
            hosts=settings.opensearch_hosts,
            news_index=settings.opensearch_news_index,
            digests_index=settings.opensearch_digests_index,
            username=settings.opensearch_username,
            password=settings.opensearch_password,
            use_ssl=settings.opensearch_use_ssl,
            verify_certs=settings.opensearch_verify_certs,
        )
    )
    return StreamlitQueryService(client, settings.opensearch_news_index, settings.opensearch_digests_index)


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def _ensure_feed_state() -> None:
    st.session_state.setdefault(STATE_ITEMS_KEY, [])
    st.session_state.setdefault(STATE_CURSOR_KEY, None)
    st.session_state.setdefault(STATE_HAS_MORE_KEY, True)
    st.session_state.setdefault(STATE_SOURCE_KEY, "")
    st.session_state.setdefault(STATE_CLASS_KEY, "")


def _reset_feed_state(source: str, class_label: str) -> None:
    st.session_state[STATE_ITEMS_KEY] = []
    st.session_state[STATE_CURSOR_KEY] = None
    st.session_state[STATE_HAS_MORE_KEY] = True
    st.session_state[STATE_SOURCE_KEY] = source
    st.session_state[STATE_CLASS_KEY] = class_label


def _load_more_news(service: StreamlitQueryService, source: str, class_label: str) -> None:
    if not st.session_state.get(STATE_HAS_MORE_KEY, True):
        return

    cursor = st.session_state.get(STATE_CURSOR_KEY)
    page = service.latest_news_page(
        size=DEFAULT_BATCH_SIZE,
        cursor=cursor,
        source=source or None,
        class_label=class_label or None,
    )

    st.session_state[STATE_ITEMS_KEY] = st.session_state.get(STATE_ITEMS_KEY, []) + page.items
    st.session_state[STATE_CURSOR_KEY] = page.next_cursor
    st.session_state[STATE_HAS_MORE_KEY] = page.has_more


def _render_hourly_summary(service: StreamlitQueryService) -> None:
    st.subheader("Саммари за последний час")
    digest = service.latest_hourly_digest_for_last_hour()
    if digest is None:
        st.info("Дайджест недоступен за последний час.")
        return

    st.write(f"Окно: {_format_dt(digest.window_start)} - {_format_dt(digest.window_end)}")
    st.write(digest.summary or "Summary is pending")
    st.caption(f"Новостей в дайджесте: {digest.news_count}")


def _render_news_card(item: NewsCard) -> None:
    st.markdown(f"### {item.title}")
    st.write(
        f"Class: {item.class_label or 'n/a'} | "
        f"Published: {_format_dt(item.published_at)} | "
        f"Source: {item.source_type or 'n/a'}"
    )
    st.write(item.summary or "Summary is pending")
    with st.expander("Показать новость целиком"):
        st.write(item.raw_text or "Текст недоступен")
        st.write(f"Section: {item.section or 'n/a'}")
        st.write(f"Authors: {item.authors or 'n/a'}")
        if item.url:
            st.markdown(f"[Open source]({item.url})")

    st.divider()


def render_app() -> None:
    st.set_page_config(page_title="News Analyzer", page_icon="📰", layout="wide")
    st.title("News Analyzer")

    service = _query_service()
    _ensure_feed_state()
    _render_hourly_summary(service)
    st.divider()

    st.subheader("Лента новостей")
    source = st.selectbox("Источник", ["", "rbc"], index=0)
    class_label = st.text_input("Class label", value="")

    prev_source = st.session_state.get(STATE_SOURCE_KEY, "")
    prev_class = st.session_state.get(STATE_CLASS_KEY, "")
    if source != prev_source or class_label != prev_class:
        _reset_feed_state(source=source, class_label=class_label)

    if not st.session_state[STATE_ITEMS_KEY]:
        _load_more_news(service, source=source, class_label=class_label)

    for item in st.session_state[STATE_ITEMS_KEY]:
        _render_news_card(item)

    if st.session_state.get(STATE_HAS_MORE_KEY, False):
        if st.button("Загрузить еще"):
            _load_more_news(service, source=source, class_label=class_label)
            st.rerun()
    elif st.session_state[STATE_ITEMS_KEY]:
        st.caption("Достигнут конец списка.")
    else:
        st.info("Новости не найдены.")


if __name__ == "__main__":
    render_app()
