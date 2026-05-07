from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@task.bash(pool='duckdb_pool')
def task_execute_tsm_models():
        return "cd /home/asha/airflow/dags/asha_models && dbt build --select +std_all_tsm_survey_responses --target prod"

@task.bash(pool='duckdb_pool')
def task_create_semantic_models():
    return "cd /home/asha/airflow/dags/gold && python -m fact_tsm_responses"

@task.bash(pool='duckdb_pool')
def task_sleep():
    return "sleep 10"

@dag(
    dag_id="dag_execute_tsm_models_prod",
    schedule="0 9-18 * * 1-5", # every mon-fri between 9 and 6
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["dbt"],
    max_active_runs=1
)
def execute_models():
    t1 = task_execute_tsm_models()
    t2 = task_create_semantic_models()
    t3 = task_sleep()
    
    t1 >> t2 >> t3
    
dag_instance = execute_models()