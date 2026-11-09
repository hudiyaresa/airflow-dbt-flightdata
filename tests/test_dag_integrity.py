import json
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.timetables.simple import DatasetTriggeredTimetable

from conftest import DAGS_DIR
from helper.datasets import STG_DATASET

INCLUDE_DIR = Path(__file__).resolve().parents[1] / "include"


@pytest.fixture(scope="session")
def dag_bag():
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dag_bag):
    assert dag_bag.import_errors == {}


def test_expected_dags_exist(dag_bag):
    assert {"flights_staging_pipeline", "flights_warehouse_pipeline"} <= set(dag_bag.dag_ids)


def test_staging_runs_extract_then_load_then_publish(dag_bag):
    dag = dag_bag.get_dag("flights_staging_pipeline")
    assert dag.max_active_runs == 1
    assert dag.default_args["on_failure_callback"] is not None

    publish = dag.get_task("publish_staging")
    assert [d.uri for d in publish.outlets] == [STG_DATASET.uri]
    assert all(t.task_id.startswith("load.") for t in publish.upstream_list)


def test_staging_no_longer_triggers_warehouse_directly(dag_bag):
    dag = dag_bag.get_dag("flights_staging_pipeline")
    assert not any(t.task_type == "TriggerDagRunOperator" for t in dag.tasks)


def test_warehouse_is_scheduled_by_staging_dataset(dag_bag):
    dag = dag_bag.get_dag("flights_warehouse_pipeline")
    assert isinstance(dag.timetable, DatasetTriggeredTimetable)
    assert STG_DATASET.uri in {uri for uri, _ in dag.timetable.dataset_condition.iter_datasets()}


def test_warehouse_runs_dbt_tests(dag_bag):
    dag = dag_bag.get_dag("flights_warehouse_pipeline")
    task_ids = {t.task_id for t in dag.tasks}
    assert any(task_id.endswith("test") for task_id in task_ids)
    assert any("fct_flight_activity" in task_id for task_id in task_ids)


def test_variables_file_matches_dag_defaults():
    from flights_staging_pipeline.tasks.main import DEFAULT_TABLE_PKEYS, TABLES_LOAD_ORDER

    variables = json.loads((INCLUDE_DIR / "variables.json").read_text())
    assert variables["incremental"] is False
    assert variables["tables_to_load"] == DEFAULT_TABLE_PKEYS
    assert set(variables["tables_to_extract"]) == set(TABLES_LOAD_ORDER)
