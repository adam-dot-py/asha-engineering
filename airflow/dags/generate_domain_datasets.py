import json
from domain_queries.dim__tsm_scales import create_domain_tsm_scales_table
from domain_queries.dim__tsm_scale_responses import create_domain_tsm_scale_responses_table
from domain_queries.dim__tsm_questions import create_domain_tsm_questions_table
from domain_queries.fact__tsm_responses import create_fact_tsm_responses
from domain_queries.fact__tsm__comments import create_fact_tsm_comments
from domain_queries.fact__tenant_records import create_fact_tenant_records
from domain_queries.fact__remittance import transform_dbo_remittance_data
from domain_queries.fact__clawbacks import transform_dbo_clawbacks_data
from domain_queries.fact__voids import transform_dbo_voids_data
from domain_queries.fact__support_notes import transform_dbo_support_notes_data
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

@task
def generate_domain_tsm_scales_table():
    create_domain_tsm_scales_table(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        domain_table='dim__tsm_scales'
    )

@task
def generate_domain_tsm_scale_responses_table():
    create_domain_tsm_scale_responses_table(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        domain_table='dim__tsm_scale_responses'
    )

@task
def generate_domain_tsm_questions_table():
    create_domain_tsm_questions_table(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        domain_table='dim__tsm_questions'
    )
    
@task
def generate_fact_tsm_responses():
    create_fact_tsm_responses(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table='tsm_responses', 
        domain_table='fact__tsm_responses'
    )

@task
def generate_fact_tsm_comments():
    create_fact_tsm_comments(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table='tsm_responses', 
        domain_table='fact__tsm_comments'
    )

@task
def generate_fact_tenant_records():
    create_fact_tenant_records(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table='tenant_data', 
        domain_table='fact__tenant_records'
    )

@task
def generate_fact_remittance_data():
    transform_dbo_remittance_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table_name='dbo_remittance', 
        domain_table_name='fact__remittance'
    )

@task
def generate_fact_clawbacks_data():
    transform_dbo_clawbacks_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table_name='dbo_clawbacks', 
        domain_table_name='fact__clawbacks'
    )

@task
def generate_fact_voids_data():
    transform_dbo_voids_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table_name='dbo_voids', 
        domain_table_name='fact__voids'
    )

@task
def generate_fact_support_notes_data():
    transform_dbo_support_notes_data(
        host=host, 
        user=user, 
        root_pass=root_pass, 
        base_table_name='dbo_support_notes', 
        domain_table_name='fact__support_notes'
    )

@dag(
    dag_id="generate_domain_datasets",
    schedule="*/20 * * * *", # every 20 minutes
    start_date=datetime(2025, 5, 2),
    catchup=False,
    tags=["domain_data"]
)
def generate_domain_data():
     
    domain_scales = generate_domain_tsm_scales_table()
    domain_scale_responses = generate_domain_tsm_scale_responses_table()
    domain_questions = generate_domain_tsm_questions_table()
    fact_responses = generate_fact_tsm_responses()
    fact_comments = generate_fact_tsm_comments()
    fact_tenant_records = generate_fact_tenant_records()
    fact_remittance = generate_fact_remittance_data()
    fact_clawbacks = generate_fact_clawbacks_data()
    fact_voids = generate_fact_voids_data()
    fact_support_notes = generate_fact_support_notes_data()
    
    domain_tasks = [domain_scales, domain_scale_responses, domain_questions]
    fact_tasks = [fact_responses, fact_comments, fact_tenant_records, fact_remittance, fact_clawbacks, fact_voids, fact_support_notes]
    
    # domain tables need to update before facts
    for d in domain_tasks:
        for f in fact_tasks:
            d >> f
            
dag_instance = generate_domain_data()