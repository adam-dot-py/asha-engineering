# packages
import duckdb
import time
import pandas as pd
import mysql.connector
import json
from functools import wraps

def replace_text(text):
    """Escapes single quotes and handles other special characters."""
    if isinstance(text, str):
        return text.replace("'", "''")  # Double single quotes for MySQL
    return text

# import motherduck token and target source config
target_source_config = "/home/asha/airflow/target-source-config.json"
server_config = "/home/asha/airflow/duckdb-config.json"
    
with open(target_source_config, "r") as t_con:
    target_config = json.load(t_con)

with open(server_config, "r") as fp:
    config = json.load(fp)
token = config['token']

def log_execution(func):
    """
    """
    
    @wraps(func)
    def etl_task_time(*args, **kwargs):
        start_time = time.time()
        print(f"Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        print(f"Finished '{func.__name__}' in {time.time() - start_time} seconds.")
        return result

    return etl_task_time

def motherduck_connection(token):
    def connection_decorator(func):
        con = duckdb.connect(f'md:?motherduck_token={token}')
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # pass con as a keyword argument for use in other functions
            return func(*args, con=con, **kwargs)
    
        return wrapper
    return connection_decorator

@log_execution
@motherduck_connection(token=token)
def create_fact_tenant_records(bronze_schema, gold_schema, bronze_table_name, gold_table_name, con, **kwargs) -> None:
    """_docstring
    """
    
    # connect to motherduck
    con.sql("USE asha_production;")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema};")
    
    # get the bronze table
    df = con.sql(f"SELECT * FROM {bronze_schema}.{bronze_table_name};").df()
    
    # Use groupby + transform to get the latest known value for each tenant
    helper_columns = []
    target_columns = ['SexualOrientation', 'ReferralAgency', 'GroupedReferralAgency', 'Age', 'Gender', 'Religion', 'Ethnicity', 'SpokenLanguage', 'Nationality', 'Disability', 'LengthOfStay', 'RiskAssessment']
    isDifferent_columns = []
    last_columns = []
    for col in target_columns:
        helper_name = f"latest_{col}"
        last_name = f"last_{col}"
        isDifferent_name = f'isDifferent_{col}'
        
        # get the latest value and first ever values
        df[helper_name] = df.groupby('Tenant_SK')[col].transform('first')
        df[last_name] = df.groupby('Tenant_SK')[col].transform('last')
        
        # update the target column with the latest value
        df[col] = df[helper_name]
        
        # track if different per target column
        df[isDifferent_name] = (df[helper_name] != df[last_name]).astype(int)
        helper_columns.append(helper_name)
        isDifferent_columns.append(isDifferent_name)
        last_columns.append(last_name)
    
    # in this section we want to check if any of what should be static values have changed between latest and first
    check_columns = ['SexualOrientation', 'Gender', 'Religion', 'Ethnicity', 'SpokenLanguage', 'Nationality', 'Disability']
    df['isDifferentOverall'] = df[[f'isDifferent_{col}' for col in check_columns]].any(axis=1).astype(int) # cool
    
    table_schema = [
        'Tenant_SK',
        'PropertyAddress',
        'Room',
        'FirstName',
        'MiddleName',
        'LastName',
        'DateOfBirth',
        'NINumber',
        'CheckinDate',
        'CheckoutDate',
        'NewHBClaim',
        'HBClaimRefNumber', 
        'ReferralAgency',
        'GroupedReferralAgency',
        'Age',
        'Gender', 
        'Religion',
        'Ethnicity', 
        'Nationality', 
        'Disability',
        'SexualOrientation',
        'SpokenLanguage', 
        'RiskAssessment',
        'LengthOfStay',
        'CycleNumber',
        'CycleNumberValue',
        'Source',
        'ProviderName',
        'isDifferentOverall',
        'isDifferent_SexualOrientation',
        'isDifferent_Gender',
        'isDifferent_Religion',
        'isDifferent_Ethnicity',
        'isDifferent_SpokenLanguage',
        'isDifferent_Nationality',
        'isDifferent_Disability',
        'LoadDate'
    ]
    
    column_data_types = {
        'Tenant_SK' : 'VARCHAR (100)',
        'PropertyAddress' : 'VARCHAR (100)',
        'Room' : 'VARCHAR(30)', 
        'FirstName' : 'VARCHAR(100)', 
        'MiddleName': 'VARCHAR(100)', 
        'LastName': 'VARCHAR(100)',
        'DateOfBirth' : 'VARCHAR(30)', 
        'NINumber' : 'VARCHAR(30)', 
        'CheckinDate' : 'VARCHAR(30)', 
        'CheckoutDate': 'VARCHAR(30)' ,
        'NewHBClaim': 'VARCHAR(30)',
        'HBClaimRefNumber': 'VARCHAR(100)', 
        'ReferralAgency': 'VARCHAR(100)',
        'GroupedReferralAgency' : 'VARCHAR(100)',
        'Age' : "VARCHAR(10)", 
        'Gender' : 'VARCHAR(30)', 
        'Religion' : 'VARCHAR(200)',
        'Ethnicity' : 'VARCHAR(200)', 
        'Nationality' : 'VARCHAR(200)', 
        'Disability' : 'VARCHAR(200)', 
        'SexualOrientation' : 'VARCHAR(100)',
        'SpokenLanguage': 'VARCHAR(100)', 
        'RiskAssessment': 'VARCHAR(100)', 
        'LengthOfStay': 'VARCHAR(100)', 
        'CycleNumber' : 'VARCHAR(100)',
        'CycleNumberValue' : 'INT',
        'Source' : 'VARCHAR(100)',
        'ProviderName' : 'VARCHAR(100)',
        'isDifferentOverall' : 'INT',
        'isDifferent_SexualOrientation' : 'INT',
        'isDifferent_Gender' : 'INT',
        'isDifferent_Religion' : 'INT',
        'isDifferent_Ethnicity' : 'INT',
        'isDifferent_SpokenLanguage' : 'INT',
        'isDifferent_Nationality' : 'INT',
        'isDifferent_Disability' : 'INT',
        'LoadDate': 'DATETIME'
    }

    # align to table schema
    df = df[table_schema]
    column_headers = [f"{col} {data_type}" for col, data_type in column_data_types.items()]
    column_headers_string = ", ".join(column_headers)
    
    # write to motherduck
    con.sql(f"CREATE OR REPLACE TABLE {gold_schema}.{gold_table_name} AS SELECT * FROM df;")
    con.close()

if __name__ == '__main__':
    
    # this is the ETL task
    bronze_schema = 'bronze'
    gold_schema = 'gold'
    bronze_table_name = 'tenant_data'
    gold_table_name = 'fact__tenant_records'

    create_fact_tenant_records(
        bronze_schema=bronze_schema,
        gold_schema=gold_schema,
        bronze_table_name=bronze_table_name,
        gold_table_name=gold_table_name
    )