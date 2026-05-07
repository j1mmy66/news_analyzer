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
            if isinstance(other, list):
                for task in other:
                    captured.setdefault("edges", []).append((self.task_id, task.task_id))
                return other
            captured.setdefault("edges", []).append((self.task_id, other.task_id))
            return other

        def __rrshift__(self, other):
            if isinstance(other, list):
                for task in other:
                    captured.setdefault("edges", []).append((task.task_id, self.task_id))
                return self
            captured.setdefault("edges", []).append((other.task_id, self.task_id))
            return self

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


def test_unified_pipeline_dag_contract(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_airflow_stubs(monkeypatch, captured)

    runpy.run_path("dags/news_unified_pipeline_dag.py")

    dag_kwargs = captured["dag_kwargs"]
    assert dag_kwargs["dag_id"] == "news_unified_pipeline"
    assert dag_kwargs["schedule"] == "*/30 * * * *"
    assert dag_kwargs["catchup"] is False
    assert dag_kwargs["max_active_runs"] == 1
    assert dag_kwargs["tags"] == ["news", "unified", "pipeline"]

    operators = captured["operators"]
    assert [operator["task_id"] for operator in operators] == [
        "rbc_ingest",
        "lenta_ingest",
        "ingest_gate",
        "semantic_dedup",
        "ner_and_classification",
        "item_summaries",
        "hourly_digest",
        "refresh_ner_entity_metrics",
    ]

    ingest_gate = next(operator for operator in operators if operator["task_id"] == "ingest_gate")
    assert ingest_gate["trigger_rule"] == "all_done"

    assert captured["edges"] == [
        ("rbc_ingest", "ingest_gate"),
        ("lenta_ingest", "ingest_gate"),
        ("ingest_gate", "semantic_dedup"),
        ("semantic_dedup", "ner_and_classification"),
        ("ner_and_classification", "item_summaries"),
        ("item_summaries", "hourly_digest"),
        ("ner_and_classification", "refresh_ner_entity_metrics"),
    ]


def test_ingest_gate_policy(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_airflow_stubs(monkeypatch, captured)
    dag_module = runpy.run_path("dags/news_unified_pipeline_dag.py")
    run_ingest_gate = dag_module["run_ingest_gate"]

    class _TIStub:
        def __init__(self, values: dict[str, object]) -> None:
            self._values = values

        def xcom_pull(self, task_ids: str):
            return self._values[task_ids]

    result = run_ingest_gate(
        _TIStub(
            {
                "rbc_ingest": {"status": "failed"},
                "lenta_ingest": {"status": "success"},
            }
        )
    )
    assert result["status"] == "success"
    assert result["succeeded_sources"] == ["lenta"]

    try:
        run_ingest_gate(
            _TIStub(
                {
                    "rbc_ingest": {"status": "failed"},
                    "lenta_ingest": {"status": "failed"},
                }
            )
        )
    except RuntimeError as exc:
        assert "Both ingest tasks failed" in str(exc)
    else:
        raise AssertionError("Expected RuntimeError when both ingest tasks fail")


def test_run_ingest_safely_returns_failed_status(monkeypatch) -> None:
    captured: dict[str, object] = {}
    _install_airflow_stubs(monkeypatch, captured)
    dag_module = runpy.run_path("dags/news_unified_pipeline_dag.py")
    run_ingest_safely = dag_module["_run_ingest_safely"]

    def _failing_ingest() -> int:
        raise RuntimeError("boom")

    result = run_ingest_safely("rbc", _failing_ingest)
    assert result["status"] == "failed"
    assert result["source"] == "rbc"
    assert result["error"] == "RuntimeError"
