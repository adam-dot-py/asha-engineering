from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@task.bash(pool='duckdb_pool')
def task_execute_tenant_models():
        return "cd /home/asha/airflow/dags/asha_models && dbt build --select +latest_tenant_data --target prod"

@task.bash(pool='duckdb_pool')
def task_sleep():
    return "sleep 10"

@dag(
    dag_id="dag_execute_tenant_models_prod",
    schedule="0 9-18 * * 1-5", # every mon-fri between 9 and 6
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["dbt"],
    max_active_runs=1
)
def execute_models():
    t1 = task_execute_tenant_models()
    t2 = task_sleep()
    
    t1 >> t2
    
dag_instance = execute_models()