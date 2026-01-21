from pathlib import Path


def test_streamlit_pages_exist() -> None:
    assert Path("src/news_analyzer/apps/streamlit/app.py").exists()
    assert Path("src/news_analyzer/apps/streamlit/pages/feed.py").exists()
    assert Path("src/news_analyzer/apps/streamlit/pages/hourly_digest.py").exists()
