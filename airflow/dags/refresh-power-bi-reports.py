from datetime import datetime
from airflow.decorators import dag, task

# Function to run PowerShell script
powershell_path = "pwsh /home/asha/airflow/dags/custom_functions/refresh-power-bi-reports.ps1"
@task.bash
def run_powershell():
    return powershell_path

@dag(
    dag_id="refresh_power_bi_reports",
    schedule="@hourly",
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["power_bi"]
)
def refresh():
    run_powershell()
   
dag_instance = refresh()