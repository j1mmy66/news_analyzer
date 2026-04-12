from __future__ import annotations

import runpy
import sys
import types


def _install_airflow_stubs(monkeypatch, captured: dict[str, object]) -> None:
    class _FakeDAG:
        def __init__(self, **kwargs) -> None:
            captured["dag_kwargs"] = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    class _Task:
        def __init__(self, task_id: str) -> None:
            self.task_id = task_id

        def __rshift__(self, other):
            captured.setdefault("edges", []).append((self.task_id, other.task_id))
            return other

    def _python_operator(**kwargs):
        captured.setdefault("operators", []).append(kwargs)
        return _Task(kwargs["task_id"])

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


def test_summaries_dag_has_dedup_then_item_then_hourly(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_airflow_stubs(monkeypatch, captured)

    runpy.run_path("dags/summaries_dag.py")

    dag_kwargs = captured["dag_kwargs"]
    assert dag_kwargs["dag_id"] == "news_summaries"

    operators = captured["operators"]
    assert [operator["task_id"] for operator in operators] == ["semantic_dedup", "item_summaries", "hourly_digest"]
    assert captured["edges"] == [("semantic_dedup", "item_summaries"), ("item_summaries", "hourly_digest")]


def test_semantic_dedup_dag_import_contract(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_airflow_stubs(monkeypatch, captured)

    runpy.run_path("dags/semantic_dedup_dag.py")

    dag_kwargs = captured["dag_kwargs"]
    assert dag_kwargs["dag_id"] == "news_semantic_dedup"
    assert dag_kwargs["schedule"] == "*/10 * * * *"
    assert dag_kwargs["tags"] == ["dedup", "semantic", "news"]

    operators = captured["operators"]
    assert len(operators) == 1
    assert operators[0]["task_id"] == "semantic_dedup"
