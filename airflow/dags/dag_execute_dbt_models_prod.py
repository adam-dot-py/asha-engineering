from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@task.bash(pool='duckdb_pool')
def task_execute_seeds():
        return "cd /home/asha/airflow/dags/asha_models && dbt seed --target prod"

@task.bash(pool='duckdb_pool')
def task_execute_loading():
        return "cd /home/asha/airflow/dags/asha_models && dbt build --select 01_Loading --target prod"
    
@task.bash(pool='duckdb_pool')
def task_execute_staging():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 02_Staging --target prod"

@task.bash(pool='duckdb_pool')
def task_execute_snapshots():
    return "cd /home/asha/airflow/dags/asha_models && dbt snapshot --target prod"

@task.bash(pool='duckdb_pool')
def task_execute_silver():
    return "cd /home/asha/airflow/dags/asha_models && dbt build --select 03_Silver --target prod --exclude latest_tenant_data"

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
def task_sleep():
    return "sleep 10"

@dag(
    dag_id="dag_execute_models_prod",
    schedule="0 9-18 * * 1-5", # every mon-fri between 9 and 6
    start_date=datetime(2026, 1, 21),
    catchup=False,
    tags=["dbt"],
    max_active_runs=1
)
def execute_models():
    t1 = task_execute_seeds()
    t2 = task_execute_loading()
    t3 = task_execute_staging()
    t4 = task_execute_snapshots()
    t5 = task_execute_silver()
    t6 = task_execute_gold()
    t7 = task_build_docs()
    t8 = task_sleep()
    
    t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> trigger_cleanup >> t8
    
dag_instance = execute_models()