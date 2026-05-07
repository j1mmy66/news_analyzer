from __future__ import annotations

from pathlib import Path
import subprocess
import time
import uuid

from opensearchpy import OpenSearch
import psycopg
import pytest
import requests

from news_analyzer.storage.opensearch.client import OpenSearchConfig, build_client
from news_analyzer.storage.opensearch.indices import OpenSearchIndexManager

REPO_ROOT = Path(__file__).resolve().parents[2]
INTEGRATION_COMPOSE_FILE = Path(__file__).with_name("docker-compose.integration.yml")

OPENSEARCH_URL = "http://localhost:19200"
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 15432
POSTGRES_DB = "airflow"
POSTGRES_USER = "airflow"
POSTGRES_PASSWORD = "airflow"


def _compose_cmd(project_name: str, *args: str) -> list[str]:
    return [
        "docker",
        "compose",
        "-p",
        project_name,
        "-f",
        str(INTEGRATION_COMPOSE_FILE),
        *args,
    ]


def _run_compose(project_name: str, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = _compose_cmd(project_name, *args)
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        check=True,
        text=True,
        capture_output=True,
    )


def _wait_for_opensearch(timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    session = requests.Session()
    session.trust_env = False
    try:
        while time.monotonic() < deadline:
            try:
                response = session.get(OPENSEARCH_URL, timeout=2.0)
                if response.ok:
                    return
            except Exception as exc:  # noqa: BLE001
                last_error = exc
            time.sleep(1.0)
    finally:
        session.close()
    raise RuntimeError(f"OpenSearch did not become ready at {OPENSEARCH_URL}: {last_error!r}")


def _wait_for_postgres(timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2,
            ) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
        time.sleep(1.0)
    raise RuntimeError(f"Postgres did not become ready at {POSTGRES_HOST}:{POSTGRES_PORT}: {last_error!r}")


@pytest.fixture(scope="session")
def integration_project_name() -> str:
    return f"newsanalyzer_it_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session", autouse=True)
def integration_stack(integration_project_name: str):
    try:
        _run_compose(integration_project_name, "up", "-d", "opensearch", "postgres")
    except subprocess.CalledProcessError as exc:
        subprocess.run(
            _compose_cmd(integration_project_name, "down", "-v", "--remove-orphans"),
            cwd=REPO_ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        raise RuntimeError(
            "Failed to start integration docker stack.\n"
            f"Command: {' '.join(exc.cmd)}\n"
            f"STDOUT:\n{exc.stdout}\n"
            f"STDERR:\n{exc.stderr}"
        ) from exc

    try:
        _wait_for_opensearch()
        _wait_for_postgres()
    except Exception:
        subprocess.run(
            _compose_cmd(integration_project_name, "down", "-v", "--remove-orphans"),
            cwd=REPO_ROOT,
            check=False,
            text=True,
            capture_output=True,
        )
        raise

    try:
        yield {
            "opensearch_url": OPENSEARCH_URL,
            "postgres_host": POSTGRES_HOST,
            "postgres_port": POSTGRES_PORT,
            "postgres_db": POSTGRES_DB,
            "postgres_user": POSTGRES_USER,
            "postgres_password": POSTGRES_PASSWORD,
        }
    finally:
        subprocess.run(
            _compose_cmd(integration_project_name, "down", "-v", "--remove-orphans"),
            cwd=REPO_ROOT,
            check=False,
            text=True,
            capture_output=True,
        )


@pytest.fixture
def unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


@pytest.fixture
def opensearch_client(integration_stack: dict[str, object]) -> OpenSearch:
    client = build_client(
        OpenSearchConfig(
            hosts=[str(integration_stack["opensearch_url"])],
            news_index="news_items",
            digests_index="hourly_digests",
            username=None,
            password=None,
            use_ssl=False,
            verify_certs=False,
        )
    )
    if not client.ping():
        raise RuntimeError("OpenSearch ping failed in integration fixture")
    return client


@pytest.fixture
def indexed_os_names(opensearch_client: OpenSearch, unique_suffix: str):
    news_index = f"it_news_{unique_suffix}"
    digests_index = f"it_digests_{unique_suffix}"
    state_index = f"it_state_{unique_suffix}"

    manager = OpenSearchIndexManager(opensearch_client)
    manager.ensure(news_index, "news.json")
    manager.ensure(digests_index, "hourly_digests.json")
    opensearch_client.indices.refresh(index=news_index)
    opensearch_client.indices.refresh(index=digests_index)
    yield {
        "news_index": news_index,
        "digests_index": digests_index,
        "state_index": state_index,
    }

    for index_name in (news_index, digests_index, state_index):
        if opensearch_client.indices.exists(index=index_name):
            opensearch_client.indices.delete(index=index_name)


@pytest.fixture
def pg_conn_kwargs(integration_stack: dict[str, object]) -> dict[str, object]:
    return {
        "host": str(integration_stack["postgres_host"]),
        "port": int(integration_stack["postgres_port"]),
        "dbname": str(integration_stack["postgres_db"]),
        "user": str(integration_stack["postgres_user"]),
        "password": str(integration_stack["postgres_password"]),
    }


@pytest.fixture
def pg_table_name(unique_suffix: str) -> str:
    return f"ner_entity_metrics_it_{unique_suffix}"


@pytest.fixture
def pg_table_cleanup(pg_conn_kwargs: dict[str, object], pg_table_name: str):
    yield pg_table_name
    with psycopg.connect(**pg_conn_kwargs, autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {pg_table_name}")
