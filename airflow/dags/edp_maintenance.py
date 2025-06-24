# packages
import json
from datetime import datetime
from airflow.decorators import dag, task

# get server config details
server_config = "/home/asha/airflow/server-config.json"

with open(server_config, 'r') as fp:
    config = json.load(fp)

host = config.get('host')
user = config.get('user')
root_pass = config.get('root_pass')

@task.bash
def edp_optimise_mysql():
    
    # optimises the database 
    return f"mysqlcheck -u {user} -p{root_pass} --all-databases --auto-repair"

@dag(
    dag_id="edp_optimisation",
    schedule="@daily",
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["optimisation"]
)

def optimise():
    edp_optimise_mysql()

dag_instance = optimise()