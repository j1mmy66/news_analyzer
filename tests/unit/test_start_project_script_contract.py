from pathlib import Path
import stat


def test_start_project_script_exists_and_is_executable() -> None:
    script = Path("scripts/start_project.sh")

    assert script.exists()
    assert script.stat().st_mode & stat.S_IXUSR


def test_start_project_script_mentions_core_services() -> None:
    script = Path("scripts/start_project.sh").read_text(encoding="utf-8")

    assert "airflow-init" in script
    assert "airflow-webserver" in script
    assert "airflow-scheduler" in script
    assert "opensearch-dashboards" in script
    assert "news_unified_pipeline" in script
