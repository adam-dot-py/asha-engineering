# packages
import json
from automation.send_lease_email import send_lease_expiry_notification
from datetime import datetime
from airflow.decorators import dag, task

# import motherduck token and target source config
server_config = "/home/asha/airflow/duckdb-config.json"

with open(server_config, "r") as fp:
    config = json.load(fp)
   
token = config['token']
schema = 'bronze'
base_table_name = 'dbo_lease_database'

@task
def send_email_task():
    send_lease_expiry_notification(
    token=token,
    schema=schema, 
    base_table_name=base_table_name
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