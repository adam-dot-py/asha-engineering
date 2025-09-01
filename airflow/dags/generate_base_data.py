import json
from pathlib import Path
from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from base_queries.get_tsm_data import create_base_tsm_table
from base_queries.get_remittance_data import extract_dbo_remittance_data
from base_queries.get_property_submissions_data import extract_dbo_property_data
from base_queries.get_hqi_data import extract_dbo_hqi_data
from base_queries.get_voids_data import extract_dbo_voids_data
from base_queries.get_master_property_database import extract_dbo_property_database_data
from base_queries.get_leavers_data import extract_dbo_leavers_data
from base_queries.get_lease_terminations_data import extract_dbo_lease_terminations_data
from base_queries.get_dal_properties_data import extract_dbo_dal_properties_data
from base_queries.get_dal_summary_data import extract_dbo_dal_summary_data
from base_queries.get_rc_ratio_data import extract_dbo_rc_ratio_data
from base_queries.get_gross_surplus import extract_dbo_gross_surplus
from base_queries.get_clawbacks_data import extract_dbo_clawback_data
from base_queries.get_piop import extract_dbo_piop

# import motherduck token and target source config
target_source_config = "/home/asha/airflow/target-source-config.json"
server_config = "/home/asha/airflow/duckdb-config.json"
    
with open(target_source_config, "r") as t_con:
    target_config = json.load(t_con)

with open(server_config, "r") as fp:
    config = json.load(fp)
    
token = config['token']
target_source_path = target_config.get("target_source_path")
schema = "bronze"

# this is the path to the tsm-responses file
tsm_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-responses.xlsx")
tsm_sea_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-sea-responses.xlsx")

@task
def generate_tsm_responses():
    create_base_tsm_table(
        token=token,
        schema=schema,
        table_name='tsm_responses', 
        tsm_file=tsm_file, 
        tsm_sea_file=tsm_sea_file
    )
    
@task
def generate_dbo_remittance():
    extract_dbo_remittance_data(
        token=token,
        schema=schema,
        table_name='dbo_remittance', 
        target_source_path=target_source_path, 
        target_sheet='Remittances'
    )

@task
def generate_dbo_clawback_data():
    extract_dbo_clawback_data(
        token=token,
        schema=schema, 
        table_name='dbo_clawbacks', 
        target_source_path=target_source_path, 
        target_sheet='Clawbacks'
    )

@task
def generate_dbo_property_data():
    extract_dbo_property_data(
        token=token,
        schema=schema,
        table_name='dbo_property_submissions', 
        target_source_path=target_source_path, 
        target_sheet='Property Sub'
    )

@task
def generate_dbo_hqi_data():
    extract_dbo_hqi_data(
        token=token,
        schema=schema,
        table_name='dbo_hqi', 
        target_source_path=target_source_path, 
        target_sheet='HQIs'
    )

@task
def generate_dbo_voids_data():
    extract_dbo_voids_data(
        token=token,
        schema=schema,
        table_name='dbo_voids', 
        target_source_path=target_source_path, 
        target_sheet='Voids'
    )

@task
def generate_dbo_property_database_data():
    extract_dbo_property_database_data(
        token=token,
        schema=schema, 
        table_name='dbo_property_database', 
        target_source_path=target_source_path, 
        target_sheet='Master Property Database'
    )

@task
def generate_dbo_leavers_data():
    extract_dbo_leavers_data(
        token=token,
        schema=schema,
        table_name='dbo_leavers', 
        target_source_path=target_source_path, 
        target_sheet='Leavers'
    )
    
@task
def generate_dbo_lease_terminations_data():
    extract_dbo_lease_terminations_data(
        token=token,
        schema=schema,
        table_name='dbo_lease_terminations', 
        target_source_path=target_source_path, 
        target_sheet='Lease Termination'
    )

@task
def generate_dbo_dal_properties_data():
    extract_dbo_dal_properties_data(
        token=token,
        schema=schema,
        table_name='dbo_dal_properties',
        target_source_path=target_source_path, 
        target_sheet='Properties Under SP with DAL'
    )

@task
def generate_dbo_dal_summary_data():
    extract_dbo_dal_summary_data(
        token=token,
        schema=schema,
        table_name='dbo_dal_summary_data',
        target_source_path=target_source_path, 
        target_sheet='DAL Summary'
    )

@task
def generate_dbo_rc_ratio_data():
    extract_dbo_rc_ratio_data(
        token=token,
        schema=schema,
        table_name='dbo_rc_ratio_data', 
        target_source_path=target_source_path, 
        target_sheet='RC Ratio'
    )

@task
def generate_dbo_gross_surplus():
    extract_dbo_gross_surplus(
        token=token,
        schema=schema,
        table_name='dbo_gross_surplus', 
        target_source_path=target_source_path, 
        target_sheet='Gross Surplus'
    )

@task
def generate_dbo_piop():
    extract_dbo_piop(
        token=token,
        schema=schema,
        table_name='dbo_piop', 
        target_source_path=target_source_path, 
        target_sheet='PIOP'
    )
    
@dag(
    dag_id="generate_base_data",
    schedule="*/15 * * * *", # every 15 minutes
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["base_data"]
)
def generate_base_data():
    op1 = generate_tsm_responses()
    op2 = generate_dbo_remittance()
    op3 = generate_dbo_clawback_data()
    op4 = generate_dbo_property_data()
    op5 = generate_dbo_hqi_data()
    op6 = generate_dbo_voids_data()
    op7 = generate_dbo_property_database_data()
    op8 = generate_dbo_leavers_data()
    op9 = generate_dbo_lease_terminations_data()
    op10 = generate_dbo_dal_properties_data()
    op11 = generate_dbo_dal_summary_data()
    op12 = generate_dbo_rc_ratio_data()
    op13 = generate_dbo_gross_surplus()
    op14 = generate_dbo_piop()
    
    chain(op1, op2, op3, op4, op5, op6, op7, op8, op9, op10, op11, op12, op13)
    
dag_instance = generate_base_data()