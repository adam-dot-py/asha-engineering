from airflow.decorators import dag, task
from datetime import datetime

@task.bash(pool='duckdb_pool')
def task_execute_seeds():
        return "cd /home/asha/airflow/dags/asha_models && dbt seed --target dev"

@task.bash(pool='duckdb_pool')
def task_execute_loading():
        return "cd /home/asha/airflow/dags/asha_models && dbt build --select 01_Loading --target dev"
    
@task.bash(pool='duckdb_pool')
def task_execute_staging():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 02_Staging --target dev"

@task.bash(pool='duckdb_pool')
def task_execute_snapshots():
    return "cd /home/asha/airflow/dags/asha_models && dbt snapshot --target dev"

@task.bash(pool='duckdb_pool')
def task_execute_silver():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 03_Silver --target dev --exclude latest_tenant_data"

@task.bash(pool='duckdb_pool')
def task_execute_gold():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 04_Gold --target dev"

@task.bash(pool='duckdb_pool')
def task_close_duckdb_connection():
    return "/home/asha/airflow_env/bin/python -c \"import duckdb; conn = duckdb.connect('/home/asha/data_lake/asha_prod.duckdb'); conn.execute('CHECKPOINT'); conn.close(); print('DuckDB checkpoint complete and connection closed')\""

@dag(
    dag_id="dag_execute_models_dev",
    schedule="@daily", # every mon-fri between 9 and 6
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["dbt"]
)
def execute_models():
    t1 = task_execute_seeds()
    t2 = task_execute_loading()
    t3 = task_execute_staging()
    t4 = task_execute_snapshots()
    t5 = task_execute_silver()
    t6 = task_execute_gold()
    c1 = task_close_duckdb_connection.override(task_id="close_duckdb_after_seeds")()
    c2 = task_close_duckdb_connection.override(task_id="close_duckdb_after_loading")()
    c3 = task_close_duckdb_connection.override(task_id="close_duckdb_after_staging")()
    c4 = task_close_duckdb_connection.override(task_id="close_duckdb_after_snapshots")()
    c5 = task_close_duckdb_connection.override(task_id="close_duckdb_after_silver")()
    c6 = task_close_duckdb_connection.override(task_id="close_duckdb_after_gold")()
    
    t1 >> c1 >> t2 >> c2 >> t3 >> c3 >> t4 >> c4 >> t5 >> c5 >> t6 >> c6
    
dag_instance = execute_models()