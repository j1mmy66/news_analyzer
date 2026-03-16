from pathlib import Path


def test_superset_assets_exist() -> None:
    base = Path("src/news_analyzer/apps/dashboard/superset/assets")
    assert (base / "metadata.yaml").exists()
    assert (base / "databases/news_analyzer_postgres.yaml").exists()
    assert (base / "datasets/news_analyzer_postgres/ner_entity_metrics.yaml").exists()
    assert (base / "charts/top_10_entities_3h.yaml").exists()
    assert (base / "charts/top_10_entities_24h.yaml").exists()
    assert (base / "charts/top_30_entities_table.yaml").exists()
    assert (base / "dashboards/ner_entities_overview.yaml").exists()


def test_superset_docs_include_required_dashboard_fields() -> None:
    datasets_doc = Path("src/news_analyzer/apps/dashboard/superset/datasets.md").read_text(encoding="utf-8")
    charts_doc = Path("src/news_analyzer/apps/dashboard/superset/charts.md").read_text(encoding="utf-8")

    assert "ner_entity_metrics" in datasets_doc
    assert "entity_name" in datasets_doc
    assert "entity_type" in datasets_doc
    assert "count_3h" in datasets_doc
    assert "count_24h" in datasets_doc
    assert "last_seen_at" in datasets_doc

    assert "Top 10 Entities (3h)" in charts_doc
    assert "Top 10 Entities (24h)" in charts_doc
    assert "Top 100 Entities Table" in charts_doc
