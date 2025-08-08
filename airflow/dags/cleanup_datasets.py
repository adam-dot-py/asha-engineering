import json
from garbage_cleanup.garbage_cleanup import cleanup_data
from garbage_cleanup.fuzzy_cleanup import fuzzy_group_data
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# import motherduck token and target source config
server_config = "/home/asha/airflow/duckdb-config.json"

with open(server_config, "r") as fp:
    config = json.load(fp)
token = config['token']
    
@task
def cleanup_tenant_data():
    cleanup_data(
        token=token,
        bronze_schema='bronze',
        bronze_table_name='tenant_data'
    )
    
@task
def fuzzy_cleanup_tenant_data():
    fuzzy_group_data(
        token=token,
        bronze_schema='bronze',
        bronze_table_name='tenant_data',
        column='ReferralAgency', 
        group_column_name='GroupedReferralAgency'
    )
       
@dag(
    dag_id="cleanup_datasets",
    schedule="*/25 * * * *", # every 25 minutes
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=["optimisation"]
)
def garbage_cleanup():
    t1 = cleanup_tenant_data() 
    t2 = fuzzy_cleanup_tenant_data()

    t1 >> t2
   
dag_instance = garbage_cleanup()