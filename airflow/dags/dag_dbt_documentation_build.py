from airflow.decorators import dag, task
from datetime import datetime

@task.bash(pool='duckdb_pool')
def task_serve_documentation():
    return "cd /home/asha/airflow/dags/asha_models && dbt docs serve --target prod --port 8081"

@dag(
    dag_id="dag_dbt_documentation_build",
    schedule="@hourly", # hourly
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["dbt"]
)
def execute_models():
    t1 = task_serve_documentation()
    
    t1
    
dag_instance = execute_models()