from __future__ import annotations

import json
from pathlib import Path

from opensearchpy import OpenSearch


class OpenSearchIndexManager:
    def __init__(self, client: OpenSearch) -> None:
        self._client = client
        self._mappings_dir = Path(__file__).with_name("mappings")

    def ensure(self, index_name: str, mapping_file: str) -> None:
        if self._client.indices.exists(index=index_name):
            return

        path = self._mappings_dir / mapping_file
        body = json.loads(path.read_text(encoding="utf-8"))
        self._client.indices.create(index=index_name, body=body)
