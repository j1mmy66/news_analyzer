from pathlib import Path


def test_streamlit_app_contract_files_exist() -> None:
    assert Path("src/news_analyzer/apps/streamlit/app.py").exists()
    assert Path("src/news_analyzer/apps/streamlit/query_service.py").exists()
    assert Path("src/news_analyzer/apps/streamlit/view_models.py").exists()
