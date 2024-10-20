import pytest
from airflow.models import DagBag

@pytest.fixture
def dagbag():
    return DagBag()

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag('snowflake_to_gcs_and_bigquery_with_dbt')
    assert dag is not None, "DAG 'snowflake_to_gcs_and_bigquery_with_dbt' is not loaded"
    assert len(dag.tasks) == 6, "The DAG should have 6 tasks"

def test_dependencies(dagbag):
    dag = dagbag.get_dag('snowflake_to_gcs_and_bigquery_with_dbt')
    extract_task = dag.get_task('extract_and_upload_to_gcs')
    verify_task = dag.get_task('verify_row_count_after_extract')
    create_tables_task = dag.get_task('create_bigquery_tables')
    load_task = dag.get_task('load_all_csvs_to_bigquery')
    run_dbt_task = dag.get_task('run_dbt_models')
    run_dbt_tests_task = dag.get_task('run_dbt_tests')

    # Check task dependencies
    assert extract_task.downstream_task_ids == {'verify_row_count_after_extract'}
    assert verify_task.downstream_task_ids == {'create_bigquery_tables'}
    assert create_tables_task.downstream_task_ids == {'load_all_csvs_to_bigquery'}
    assert load_task.downstream_task_ids == {'run_dbt_models'}
    assert run_dbt_task.downstream_task_ids == {'run_dbt_tests'}
