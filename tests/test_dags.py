import json
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

ROOT = Path(__file__).resolve().parent.parent


POOLS = set(json.loads((ROOT / "pools.json").read_text()))


@pytest.fixture(scope="module")
def dagbag():
    # known_pools makes Airflow itself warn about any task that uses an undeclared pool.
    return DagBag(dag_folder=str(ROOT / "dags"), known_pools=POOLS)


def test_dags_import_without_errors_or_warnings(dagbag):
    assert dagbag.import_errors == {}
    assert not dagbag.dag_warnings
    assert set(dagbag.dag_ids) == {"shop_daily", "shop_full_refresh"}


def test_daily_runs_spark_then_dbt(dagbag):
    dag = dagbag.dags["shop_daily"]
    assert dag.get_task("clean_events").downstream_task_ids == {"dbt_build"}
    assert dag.max_active_runs == 1 and dag.catchup is False


def test_every_pod_has_a_known_pool_and_resources(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert isinstance(task, KubernetesPodOperator)
            assert task.pool in POOLS, task.task_id
            assert task.container_resources.requests["memory"], task.task_id


def test_dbt_always_goes_through_the_warehouse_pool(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            if task.image.split("/")[-1].startswith("dbt-"):
                assert task.pool == "warehouse", task.task_id


def test_run_date_comes_from_data_interval_end(dagbag):
    task = dagbag.dags["shop_daily"].get_task("clean_events")
    assert "data_interval_end" in " ".join(task.arguments)


def test_no_dag_uses_execution_date():
    for path in (ROOT / "dags").rglob("*.py"):
        code = path.read_text()
        assert "{{ execution_date" not in code and "context['execution_date']" not in code, path


def test_failure_callback_is_set_everywhere(dagbag):
    for dag in dagbag.dags.values():
        for task in dag.tasks:
            assert task.on_failure_callback, task.task_id


def test_full_refresh_is_never_scheduled(dagbag):
    dag = dagbag.dags["shop_full_refresh"]
    assert dag.schedule is None
