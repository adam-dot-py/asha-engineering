import os
import json
from garbage_cleanup.garbage_cleanup import cleanup_data
from garbage_cleanup.fuzzy_cleanup import fuzzy_group_data
from latest_queries.generate_latest_yearly_tenant_data import generate_latest_yearly_tenant_data
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task

# import server config file
server_config = "/home/asha/airflow/server-config.json"
with open(server_config, "r") as fp:
    config = json.load(fp)
    
# source target config
source_config = "/home/asha/airflow/target-source-config.json"
with open(source_config, "r") as fp:
    src_config = json.load(fp)
        
# connect to target source
target_source_path = src_config.get("target_source_path")

# this is the path to the tsm-responses file
tsm_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-responses.xlsx")
tsm_sea_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-sea-responses.xlsx")

# prepare the details to connect to the database
host = config.get("host")
user = config.get("user")
root_pass = config.get("root_pass")
db_name = "base"

@task
def cleanup_yearly_tenant_data():
    cleanup_data(
     host=host,
     user=user, 
     root_pass=root_pass, 
     base_table='yearly_tenant_data'
    )

@task
def create_latest_yearly_data_view():
    generate_latest_yearly_tenant_data(
     host=host,
     user=user, 
     root_pass=root_pass
    )
     
@dag(
    dag_id="generate_latest_views",
    schedule="@hourly",
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["views"]
)
def latest_views():
    t1 = cleanup_yearly_tenant_data()
    t2 = create_latest_yearly_data_view()
    
    t1 >> t2
   
dag_instance = latest_views()