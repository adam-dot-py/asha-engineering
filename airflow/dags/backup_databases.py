# packages
from datetime import datetime
from airflow.decorators import dag, task

@task.bash
def backup_base():
    
    # optimises the database 
    return f"/custom_functions/backup-base-database.sh"

@task.bash
def backup_domain():
    
    # optimises the database 
    return f"/custom_functions/backup-domain-database.sh"

@dag(
    dag_id="backup_databases",
    schedule="@daily",
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["optimisation"]
)

def execute_backup():
    base = backup_base()
    domain = backup_domain()
    
    base >> domain

dag_instance = execute_backup()