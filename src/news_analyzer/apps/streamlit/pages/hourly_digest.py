from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import streamlit as st

from news_analyzer.apps.streamlit.pages.feed import _query_service

MOSCOW_TZ = ZoneInfo("Europe/Moscow")


def _format_dt(value: datetime | None) -> str:
    if value is None:
        return "n/a"
    return value.astimezone(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")


def render_hourly_digest() -> None:
    st.subheader("Hourly Digest")
    digest = _query_service().latest_hourly_digest_for_last_hour()
    if digest is None:
        st.info("No hourly digest available for the last hour")
        return

    st.write(f"Window: {_format_dt(digest.window_start)} - {_format_dt(digest.window_end)}")
    st.write(digest.summary or "No summary")
    st.write(f"Items in digest: {digest.news_count}")
