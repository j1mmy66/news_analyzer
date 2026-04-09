#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

if [[ ! -x ".venv/bin/streamlit" ]]; then
  echo "Missing .venv/bin/streamlit. Create venv and install dependencies first." >&2
  exit 1
fi

echo "Ensuring OpenSearch container is running..."
docker compose up -d opensearch >/dev/null

echo "Waiting for OpenSearch on http://localhost:9200 ..."
for _ in {1..60}; do
  if curl -fsS "http://localhost:9200" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "http://localhost:9200" >/dev/null 2>&1; then
  echo "OpenSearch is not reachable on http://localhost:9200" >&2
  exit 1
fi

export PYTHONPATH="${PYTHONPATH:-src}"
export OPENSEARCH_HOSTS="${OPENSEARCH_HOSTS:-http://localhost:9200}"
export OPENSEARCH_NEWS_INDEX="${OPENSEARCH_NEWS_INDEX:-news_items}"
export OPENSEARCH_DIGESTS_INDEX="${OPENSEARCH_DIGESTS_INDEX:-hourly_digests}"

echo "Starting Streamlit on http://127.0.0.1:8501 ..."
exec .venv/bin/streamlit run src/news_analyzer/apps/streamlit/app.py \
  --server.address 127.0.0.1 \
  --server.port 8501
