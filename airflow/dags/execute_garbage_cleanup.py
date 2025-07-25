import duckdb
import json
from garbage_cleanup.garbage_cleanup import cleanup_data
from garbage_cleanup.fuzzy_cleanup import fuzzy_group_data
from pathlib import Path
from datetime import datetime
from airflow.decorators import dag, task
from functools import wraps

# import motherduck token and target source config
server_config = "/home/asha/airflow/duckdb-config.json"

with open(server_config, "r") as fp:
    config = json.load(fp)
token = config['token']

# @task
# def cleanup_yearly_tenant_data():
#     cleanup_data(
#      token=token,
#      bronze_schema='bronze',
#      bronze_table_name='yearly_tenant_data'
#     )
    
# @task
# def fuzzy_cleanup_yearly_tenant_data():
#     fuzzy_group_data(
#         token=token,
#         bronze_schema='bronze',
#         bronze_table_name='yearly_tenant_data',
#         column='ReferralAgency', 
#         group_column_name='GroupedReferralAgency'
#     )
    
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
    dag_id="garbage_cleanup",
    schedule="*/25 * * * *", # every 25 minutes
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["optimisation"]
)
def garbage_cleanup():
    # t1 = cleanup_yearly_tenant_data() 
    # t2 = fuzzy_cleanup_yearly_tenant_data()
    # t3 = cleanup_tenant_data() 
    # t4 = fuzzy_cleanup_tenant_data()
    
    # t1 >> t2
    # t3 >> t4
    
    # t1 = cleanup_yearly_tenant_data() 
    # t2 = fuzzy_cleanup_yearly_tenant_data()
    t3 = cleanup_tenant_data() 
    t4 = fuzzy_cleanup_tenant_data()
    
    # t1 >> t2
    t3 >> t4
   
dag_instance = garbage_cleanup()