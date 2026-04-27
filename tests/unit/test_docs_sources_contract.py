from pathlib import Path


def _read(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def test_readme_mentions_sources_and_degradation() -> None:
    readme = _read("README.md").lower()

    assert "rbc" in readme
    assert "lenta" in readme
    assert "lenta_news_ingest" in readme
    assert "full_text" in readme


def test_instruction_mentions_ingest_dags() -> None:
    instruction = _read("instruction.md")

    assert "rbc_news_ingest" in instruction
    assert "lenta_news_ingest" in instruction
    assert "lenta:" in instruction
