import json
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
from pathlib import Path
from datetime import datetime
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
def generate_tsm_responses():
    create_base_tsm_table(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='tsm_responses', 
        tsm_file=tsm_file, 
        tsm_sea_file=tsm_sea_file
    )
    
@task
def generate_dbo_remittance():
    extract_dbo_remittance_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_remittance', 
        target_source_path=target_source_path, 
        target_sheet='Remittances'
    )

@task
def generate_dbo_clawback_data():
    extract_dbo_clawback_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_clawbacks', 
        target_source_path=target_source_path, 
        target_sheet='Clawbacks'
    )

@task
def generate_dbo_property_data():
    extract_dbo_property_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_property_submissions', 
        target_source_path=target_source_path, 
        target_sheet='Property Sub'
    )

@task
def generate_dbo_hqi_data():
    extract_dbo_hqi_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_hqi', 
        target_source_path=target_source_path, 
        target_sheet='HQIs'
    )

@task
def generate_dbo_voids_data():
    extract_dbo_voids_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_voids', 
        target_source_path=target_source_path, 
        target_sheet='Voids'
    )

@task
def generate_dbo_property_database_data():
    extract_dbo_property_database_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_property_database', 
        target_source_path=target_source_path, 
        target_sheet='Master Property Database'
    )

@task
def generate_dbo_leavers_data():
    extract_dbo_leavers_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_leavers', 
        target_source_path=target_source_path, 
        target_sheet='Leavers'
    )
    
@task
def generate_dbo_lease_terminations_data():
    extract_dbo_lease_terminations_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_lease_terminations', 
        target_source_path=target_source_path, 
        target_sheet='Lease Termination'
    )

@task
def generate_dbo_dal_properties_data():
    extract_dbo_dal_properties_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_dal_properties',
        target_source_path=target_source_path, 
        target_sheet='Properties Under SP with DAL'
    )

@task
def generate_dbo_dal_summary_data():
    extract_dbo_dal_summary_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_dal_summary_data',
        target_source_path=target_source_path, 
        target_sheet='DAL Summary'
    )

@task
def generate_dbo_rc_ratio_data():
    extract_dbo_rc_ratio_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_rc_ratio_data', 
        target_source_path=target_source_path, 
        target_sheet='RC Ratio'
    )

@task
def generate_dbo_gross_surplus():
    extract_dbo_gross_surplus(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
        table_name='dbo_gross_surplus', 
        target_source_path=target_source_path, 
        target_sheet='Gross Surplus'
    )

@task
def generate_dbo_piop():
    extract_dbo_piop(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        db_name=db_name, 
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
    generate_tsm_responses()
    generate_dbo_remittance()
    generate_dbo_clawback_data()
    generate_dbo_property_data()
    generate_dbo_hqi_data()
    generate_dbo_voids_data()
    generate_dbo_property_database_data()
    generate_dbo_leavers_data()
    generate_dbo_lease_terminations_data()
    generate_dbo_dal_properties_data()
    generate_dbo_dal_summary_data()
    generate_dbo_rc_ratio_data()
    generate_dbo_gross_surplus()
    generate_dbo_piop()
    
dag_instance = generate_base_data()