from __future__ import annotations

import streamlit as st

from news_analyzer.apps.streamlit.query_service import StreamlitQueryService
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client


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


def render_feed() -> None:
    st.subheader("News Feed")
    source = st.selectbox("Source", ["", "rbc"], index=0)
    class_label = st.text_input("Class label")

    items = _query_service().latest_news(source=source or None, class_label=class_label or None)
    for item in items:
        st.markdown(f"### {item.get('source_metadata', {}).get('title') or item.get('external_id')}")
        st.write(f"Source: {item.get('source_type')} | Class: {item.get('class_label', 'n/a')}")
        st.write(item.get("summary") or "Summary is pending")
        link = item.get("source_metadata", {}).get("url") or item.get("source_metadata", {}).get("permalink")
        if link:
            st.markdown(f"[Open source]({link})")
        st.divider()
