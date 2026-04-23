from __future__ import annotations

from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
import types

import requests

from news_analyzer.domain.enums import ProcessingStatus, SourceType
from news_analyzer.domain.models import NormalizedNewsItem, SummaryResult
from news_analyzer.pipeline.summarize import hourly_digest_job, item_summary_job
from news_analyzer.settings.app_settings import AppSettings
from news_analyzer.storage.opensearch.repositories import NewsRepository


class _FakeEndpointState:
    def __init__(self) -> None:
        self._responses: list[tuple[int, dict[str, object]]] = []
        self.requests: list[dict[str, object]] = []
        self._lock = threading.Lock()

    def queue_response(self, status_code: int, payload: dict[str, object]) -> None:
        with self._lock:
            self._responses.append((status_code, payload))

    def next_response(self) -> tuple[int, dict[str, object]]:
        with self._lock:
            if not self._responses:
                return 200, {"choices": [{"message": {"content": "default summary"}}]}
            return self._responses.pop(0)


def _handler_factory(state: _FakeEndpointState):
    class _Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length)
            try:
                payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
            except json.JSONDecodeError:
                payload = {}
            state.requests.append(payload)

            status_code, response_payload = state.next_response()
            encoded = json.dumps(response_payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, _format: str, *_args: object) -> None:
            return None

    return _Handler


def _sample_item(external_id: str, published_at: datetime, *, cleaned_text: str) -> NormalizedNewsItem:
    return NormalizedNewsItem(
        source_type=SourceType.RBC,
        external_id=external_id,
        published_at=published_at,
        source_metadata={"url": f"https://example.com/{external_id}"},
        raw_text=cleaned_text,
        cleaned_text=cleaned_text,
    )


def _summary_settings(news_index: str, digests_index: str) -> AppSettings:
    return AppSettings(
        opensearch_hosts=["http://localhost:19200"],
        opensearch_news_index=news_index,
        opensearch_digests_index=digests_index,
        opensearch_username=None,
        opensearch_password=None,
        opensearch_use_ssl=False,
        opensearch_verify_certs=False,
        gigachat_auth_key="it-auth-key",
        gigachat_scope="GIGACHAT_API_PERS",
        gigachat_model="GigaChat",
        gigachat_timeout_seconds=5.0,
        gigachat_max_retries=2,
        gigachat_verify_ssl=False,
        classifier_model_path=Path("models/any-news-classifier"),
        ner_slovnet_model_path=Path("models/slovnet_ner_news_v1.tar"),
        ner_navec_path=Path("models/navec_news_v1_1B_250K_300d_100q.tar"),
    )


def _install_fake_gigachat_module(monkeypatch, base_url: str) -> None:
    class _FakeGigaChat:
        def __init__(self, **kwargs) -> None:
            self._timeout = float(kwargs.get("timeout", 5.0))

        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def chat(self, payload: dict[str, object]) -> dict[str, object]:
            session = requests.Session()
            session.trust_env = False
            try:
                response = session.post(f"{base_url}/chat", json=payload, timeout=self._timeout)
            finally:
                session.close()
            if response.status_code >= 400:
                raise RuntimeError(f"{response.status_code} service unavailable")
            return response.json()

    monkeypatch.setitem(sys.modules, "gigachat", types.SimpleNamespace(GigaChat=_FakeGigaChat))


def test_item_summary_job_success(monkeypatch, opensearch_client, indexed_os_names) -> None:
    news_index = indexed_os_names["news_index"]
    digests_index = indexed_os_names["digests_index"]
    repository = NewsRepository(opensearch_client, news_index)
    now = datetime.now(timezone.utc)
    assert repository.upsert_news([_sample_item("n1", now - timedelta(minutes=5), cleaned_text="Текст новости")]) == 1
    opensearch_client.indices.refresh(index=news_index)

    state = _FakeEndpointState()
    state.queue_response(200, {"choices": [{"message": {"content": "summary-ok"}}]})
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _install_fake_gigachat_module(monkeypatch, f"http://127.0.0.1:{server.server_port}")
        settings = _summary_settings(news_index, digests_index)
        monkeypatch.setattr(item_summary_job.AppSettings, "from_env", classmethod(lambda cls: settings))

        processed = item_summary_job.run_item_summary_job(limit=10)
        assert processed == 1
        opensearch_client.indices.refresh(index=news_index)
        payload = opensearch_client.get(index=news_index, id="n1")["_source"]
        assert payload["summary"] == "summary-ok"
        assert payload["summary_status"] == ProcessingStatus.SUCCESS.value
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_item_summary_job_mixed_results_continue_processing(monkeypatch, opensearch_client, indexed_os_names) -> None:
    news_index = indexed_os_names["news_index"]
    digests_index = indexed_os_names["digests_index"]
    repository = NewsRepository(opensearch_client, news_index)
    now = datetime.now(timezone.utc)
    assert repository.upsert_news(
        [
            _sample_item("n-old", now - timedelta(minutes=15), cleaned_text="Старый текст"),
            _sample_item("n-new", now - timedelta(minutes=5), cleaned_text="Новый текст"),
        ]
    ) == 2
    opensearch_client.indices.refresh(index=news_index)

    state = _FakeEndpointState()
    state.queue_response(200, {"choices": [{"message": {"content": "ok-1"}}]})
    state.queue_response(503, {"error": "temporary"})
    state.queue_response(503, {"error": "temporary"})
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _install_fake_gigachat_module(monkeypatch, f"http://127.0.0.1:{server.server_port}")
        settings = _summary_settings(news_index, digests_index)
        monkeypatch.setattr(item_summary_job.AppSettings, "from_env", classmethod(lambda cls: settings))

        processed = item_summary_job.run_item_summary_job(limit=10)
        assert processed == 2
        opensearch_client.indices.refresh(index=news_index)

        statuses = {
            opensearch_client.get(index=news_index, id="n-old")["_source"]["summary_status"],
            opensearch_client.get(index=news_index, id="n-new")["_source"]["summary_status"],
        }
        assert statuses == {ProcessingStatus.SUCCESS.value, ProcessingStatus.FAILED.value}
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_item_summary_job_set_summary_failure_does_not_stop_batch(monkeypatch, opensearch_client, indexed_os_names) -> None:
    news_index = indexed_os_names["news_index"]
    digests_index = indexed_os_names["digests_index"]
    repository = NewsRepository(opensearch_client, news_index)
    now = datetime.now(timezone.utc)
    assert repository.upsert_news(
        [
            _sample_item("n-1", now - timedelta(minutes=20), cleaned_text="Текст 1"),
            _sample_item("n-2", now - timedelta(minutes=10), cleaned_text="Текст 2"),
        ]
    ) == 2
    opensearch_client.indices.refresh(index=news_index)

    class _FailingSummaryWriteRepository:
        def __init__(self, client: object, index_name: str) -> None:
            self._delegate = NewsRepository(client, index_name)

        def get_recent_canonical_news_without_summary(self, limit: int = 100):
            rows = self._delegate.get_recent_canonical_news_without_summary(limit=limit)
            if len(rows) < 2:
                return rows + [{"external_id": "missing-doc", "cleaned_text": "Отсутствующий документ"}]
            return [rows[0], {"external_id": "missing-doc", "cleaned_text": "Отсутствующий документ"}, rows[1]]

        def set_summary(self, external_id: str, summary: SummaryResult) -> None:
            self._delegate.set_summary(external_id, summary)

    state = _FakeEndpointState()
    state.queue_response(200, {"choices": [{"message": {"content": "ok-1"}}]})
    state.queue_response(200, {"choices": [{"message": {"content": "ok-missing"}}]})
    state.queue_response(200, {"choices": [{"message": {"content": "ok-2"}}]})
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _install_fake_gigachat_module(monkeypatch, f"http://127.0.0.1:{server.server_port}")
        settings = _summary_settings(news_index, digests_index)
        monkeypatch.setattr(item_summary_job.AppSettings, "from_env", classmethod(lambda cls: settings))
        monkeypatch.setattr(item_summary_job, "NewsRepository", _FailingSummaryWriteRepository)

        processed = item_summary_job.run_item_summary_job(limit=10)
        assert processed == 2
        opensearch_client.indices.refresh(index=news_index)

        status_n1 = opensearch_client.get(index=news_index, id="n-1")["_source"]["summary_status"]
        status_n2 = opensearch_client.get(index=news_index, id="n-2")["_source"]["summary_status"]
        assert {status_n1, status_n2} == {ProcessingStatus.SUCCESS.value}
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_hourly_digest_job_success(monkeypatch, opensearch_client, indexed_os_names) -> None:
    news_index = indexed_os_names["news_index"]
    digests_index = indexed_os_names["digests_index"]
    repository = NewsRepository(opensearch_client, news_index)
    now = datetime.now(timezone.utc)
    assert repository.upsert_news(
        [
            _sample_item("h1", now - timedelta(minutes=40), cleaned_text="Текст 1"),
            _sample_item("h2", now - timedelta(minutes=20), cleaned_text="Текст 2"),
        ]
    ) == 2
    repository.set_summary(
        "h1",
        SummaryResult(summary="item-sum-1", status=ProcessingStatus.SUCCESS, error_code=None, updated_at=now),
    )
    repository.set_summary(
        "h2",
        SummaryResult(summary="item-sum-2", status=ProcessingStatus.SUCCESS, error_code=None, updated_at=now),
    )
    opensearch_client.indices.refresh(index=news_index)

    state = _FakeEndpointState()
    state.queue_response(200, {"choices": [{"message": {"content": "hourly-summary-ok"}}]})
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _install_fake_gigachat_module(monkeypatch, f"http://127.0.0.1:{server.server_port}")
        settings = _summary_settings(news_index, digests_index)
        monkeypatch.setattr(hourly_digest_job.AppSettings, "from_env", classmethod(lambda cls: settings))

        digest_id = hourly_digest_job.run_hourly_digest_job()
        assert digest_id is not None

        opensearch_client.indices.refresh(index=news_index)
        opensearch_client.indices.refresh(index=digests_index)
        digest_payload = opensearch_client.get(index=digests_index, id=digest_id)["_source"]
        assert digest_payload["summary"] == "hourly-summary-ok"
        assert set(digest_payload["news_ids"]) == {"h1", "h2"}

        doc_h1 = opensearch_client.get(index=news_index, id="h1")["_source"]
        doc_h2 = opensearch_client.get(index=news_index, id="h2")["_source"]
        assert doc_h1["hourly_digest_id"] == digest_id
        assert doc_h2["hourly_digest_id"] == digest_id
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_hourly_digest_job_content_restricted_does_not_create_digest(monkeypatch, opensearch_client, indexed_os_names) -> None:
    news_index = indexed_os_names["news_index"]
    digests_index = indexed_os_names["digests_index"]
    repository = NewsRepository(opensearch_client, news_index)
    now = datetime.now(timezone.utc)
    assert repository.upsert_news(
        [
            _sample_item("r1", now - timedelta(minutes=30), cleaned_text="Текст 1"),
            _sample_item("r2", now - timedelta(minutes=10), cleaned_text="Текст 2"),
        ]
    ) == 2
    repository.set_summary(
        "r1",
        SummaryResult(summary="item-sum-r1", status=ProcessingStatus.SUCCESS, error_code=None, updated_at=now),
    )
    repository.set_summary(
        "r2",
        SummaryResult(summary="item-sum-r2", status=ProcessingStatus.SUCCESS, error_code=None, updated_at=now),
    )
    opensearch_client.indices.refresh(index=news_index)

    state = _FakeEndpointState()
    state.queue_response(
        200,
        {"choices": [{"finish_reason": "blacklist", "message": {"content": "restricted"}}]},
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), _handler_factory(state))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        _install_fake_gigachat_module(monkeypatch, f"http://127.0.0.1:{server.server_port}")
        settings = _summary_settings(news_index, digests_index)
        monkeypatch.setattr(hourly_digest_job.AppSettings, "from_env", classmethod(lambda cls: settings))

        digest_id = hourly_digest_job.run_hourly_digest_job()
        assert digest_id is None

        opensearch_client.indices.refresh(index=news_index)
        opensearch_client.indices.refresh(index=digests_index)

        digests_hits = opensearch_client.search(
            index=digests_index,
            body={"query": {"match_all": {}}, "size": 10},
        )["hits"]["hits"]
        assert digests_hits == []

        doc_r1 = opensearch_client.get(index=news_index, id="r1")["_source"]
        doc_r2 = opensearch_client.get(index=news_index, id="r2")["_source"]
        assert "hourly_digest_id" not in doc_r1
        assert "hourly_digest_id" not in doc_r2
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
