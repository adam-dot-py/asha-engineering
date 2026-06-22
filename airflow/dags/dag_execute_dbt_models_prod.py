from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@task.bash(pool='duckdb_pool')
def task_execute_seeds():
    return "cd /home/asha/airflow/dags/asha_models && dbt seed --target prod"

@task.bash(pool='duckdb_pool')
def task_execute_loading():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 01_Loading --target prod --exclude raw_tsm_responses raw_tsm_sea_responses"
    
@task.bash(pool='duckdb_pool')
def task_execute_staging():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 02_Staging --target prod --exclude stg_tsm_responses stg_tsm_sea_responses"

@task.bash(pool='duckdb_pool')
def task_execute_snapshots():
    return "cd /home/asha/airflow/dags/asha_models && dbt snapshot --target prod"

@task.bash(pool='duckdb_pool')
def task_execute_silver():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 03_Silver --target prod --exclude latest_tenant_data std_tsm_sea_responses std_tsm_responses"

@task.bash(pool='duckdb_pool')
def task_execute_gold():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 04_Gold --target prod"

@task.bash(pool='duckdb_pool')
def task_build_docs():
    return "cd /home/asha/airflow/dags/asha_models && dbt docs generate --target prod"


trigger_cleanup = TriggerDagRunOperator(
    task_id='trigger_cleanup_datasets',
    trigger_dag_id='cleanup_datasets'
)

@task.bash(pool='duckdb_pool')
def task_close_duckdb_connection():
    return "/home/asha/airflow_env/bin/python -c \"import duckdb; conn = duckdb.connect('/home/asha/data_lake/asha_prod.duckdb'); conn.execute('CHECKPOINT'); conn.close(); print('DuckDB checkpoint complete and connection closed')\""

@dag(
    dag_id="dag_execute_models_prod",
    schedule="0 9-18 * * 1-5", # every mon-fri between 9 and 6
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["dbt"],
    max_active_runs=1,
    
)
def execute_models():
    t1 = task_execute_seeds()
    t2 = task_execute_loading()
    t3 = task_execute_staging()
    t4 = task_execute_snapshots()
    t5 = task_execute_silver()
    t6 = task_execute_gold()
    t7 = task_build_docs()
    c1 = task_close_duckdb_connection.override(task_id="close_duckdb_after_seeds")()
    c2 = task_close_duckdb_connection.override(task_id="close_duckdb_after_loading")()
    c3 = task_close_duckdb_connection.override(task_id="close_duckdb_after_staging")()
    c4 = task_close_duckdb_connection.override(task_id="close_duckdb_after_snapshots")()
    c5 = task_close_duckdb_connection.override(task_id="close_duckdb_after_silver")()
    c6 = task_close_duckdb_connection.override(task_id="close_duckdb_after_gold")()
    c7 = task_close_duckdb_connection.override(task_id="close_duckdb_after_docs")()
    
    t1 >> c1 >> t2 >> c2 >> t3 >> c3 >> t4 >> c4 >> t5 >> c5 >> t6 >> c6 >> t7 >> c7 >> trigger_cleanup
    
dag_instance = execute_models()