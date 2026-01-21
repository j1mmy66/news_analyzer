from __future__ import annotations

import streamlit as st

from news_analyzer.apps.streamlit.pages.feed import render_feed
from news_analyzer.apps.streamlit.pages.hourly_digest import render_hourly_digest

st.set_page_config(page_title="News Analyzer", page_icon="📰", layout="wide")
st.title("News Analyzer")

mode = st.sidebar.radio("View", ["Feed", "Hourly Digest"])

if mode == "Feed":
    render_feed()
else:
    render_hourly_digest()
