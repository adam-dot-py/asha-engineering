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
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 03_Silver --target dev"

@task.bash(pool='duckdb_pool')
def task_execute_gold():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 04_Gold --target dev"

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
    
    t1 >> t2 >> t3 >> t4 >> t5 >> t6
    
dag_instance = execute_models()