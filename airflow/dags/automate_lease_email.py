# packages
import json
from automation.send_lease_email import send_lease_expiry_notification
from datetime import datetime
from airflow.decorators import dag, task


schema = 'main_silver'
silver_table = 'latest_lease_database'

@task
def send_email_task():
    send_lease_expiry_notification(
    schema=schema, 
    silver_table=silver_table
)
    
@dag(
    dag_id="automate_lease_email",
    schedule="@daily",
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["functions"]
)

def execute_functions():
    send_email_task()
    
dag_instance = execute_functions()