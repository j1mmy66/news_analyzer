from __future__ import annotations

import streamlit as st

from news_analyzer.apps.streamlit.pages.feed import _query_service


def render_hourly_digest() -> None:
    st.subheader("Hourly Digest")
    digest = _query_service().latest_hourly_digest()
    if digest is None:
        st.info("No hourly digest available yet")
        return

    st.write(f"Window: {digest.get('window_start')} - {digest.get('window_end')}")
    st.write(digest.get("summary") or "No summary")
    st.write(f"Items in digest: {len(digest.get('news_ids', []))}")
