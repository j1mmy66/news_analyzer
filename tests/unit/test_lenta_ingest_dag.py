from __future__ import annotations

import runpy
import sys
import types


def test_lenta_ingest_dag_import_contract(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _FakeDAG:
        def __init__(self, **kwargs) -> None:
            captured["dag_kwargs"] = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    def _python_operator(**kwargs):
        captured.setdefault("operators", []).append(kwargs)
        return object()

    fake_airflow = types.ModuleType("airflow")
    fake_airflow.DAG = _FakeDAG

    fake_airflow_ops = types.ModuleType("airflow.operators")
    fake_airflow_ops_python = types.ModuleType("airflow.operators.python")
    fake_airflow_ops_python.PythonOperator = _python_operator

    fake_pendulum = types.ModuleType("pendulum")
    fake_pendulum.datetime = lambda *args, **kwargs: "start-date"

    monkeypatch.setitem(sys.modules, "airflow", fake_airflow)
    monkeypatch.setitem(sys.modules, "airflow.operators", fake_airflow_ops)
    monkeypatch.setitem(sys.modules, "airflow.operators.python", fake_airflow_ops_python)
    monkeypatch.setitem(sys.modules, "pendulum", fake_pendulum)

    runpy.run_path("dags/lenta_ingest_dag.py")

    dag_kwargs = captured["dag_kwargs"]
    assert dag_kwargs["dag_id"] == "lenta_news_ingest"
    assert dag_kwargs["tags"] == ["lenta", "ingest", "news"]

    operators = captured["operators"]
    assert len(operators) == 1
    assert operators[0]["task_id"] == "lenta_ingest"
