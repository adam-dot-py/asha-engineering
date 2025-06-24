# packages
import json
from automation.send_lease_email import send_lease_expiry_notification
from datetime import datetime
from airflow.decorators import dag, task

# import server config file
server_config = "/home/asha/airflow/server-config.json"

with open(server_config, "r") as fp:
    config = json.load(fp)

# prepare the details to connect to the databases
host = config.get("host")
user = config.get("user")
root_pass = config.get("root_pass")
base_table_name = 'dbo_lease_database'

@task
def send_email_task():
    send_lease_expiry_notification(
    host=host, 
    user=user, 
    root_pass=root_pass, 
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