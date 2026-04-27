from __future__ import annotations

from datetime import datetime
import logging
from zoneinfo import ZoneInfo

import streamlit as st

from news_analyzer.apps.streamlit.query_service import StreamlitQueryService
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client

MOSCOW_TZ = ZoneInfo("Europe/Moscow")
logger = logging.getLogger(__name__)


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


def render_feed() -> None:
    st.subheader("News Feed")
    source = st.selectbox("Source", ["", "rbc", "lenta"], index=0)
    class_label = st.text_input("Class label")

    try:
        page = _query_service().latest_news_page(source=source or None, class_label=class_label or None)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to load feed page from OpenSearch")
        st.error("Could not load news feed: OpenSearch is unavailable.")
        return

    for item in page.items:
        st.markdown(f"### {item.title}")
        st.write(f"Source: {item.source_type or 'n/a'} | Class: {item.class_label or 'n/a'}")
        st.write(f"Published: {_format_dt(item.published_at)}")
        st.write(item.summary or "Summary is pending")
        if item.url:
            st.markdown(f"[Open source]({item.url})")
        st.divider()
