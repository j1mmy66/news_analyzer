import json
from pathlib import Path


def test_news_mapping_contains_required_fields() -> None:
    path = Path("src/news_analyzer/storage/opensearch/mappings/news.json")
    payload = json.loads(path.read_text(encoding="utf-8"))
    properties = payload["mappings"]["properties"]

    assert "source_type" in properties
    assert "class_label" in properties
    assert "summary" in properties
    assert "entities" in properties
