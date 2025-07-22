# append custom function path and bring in lookup
import sys
sys.path.append('/home/asha/airflow/dags/custom_functions')
from lookup_support_provider import lookup_support_provider

# packages
import duckdb
import os
import json
import time
import re
import pandas as pd
from pathlib import Path
from datetime import datetime

def log_execution(func):
    """
    """
    
    def etl_task_time(*args, **kwargs):
        start_time = time.time()
        print(f"Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        print(f"Finished '{func.__name__}' in {time.time() - start_time} seconds.")
        return result

    return etl_task_time

@log_execution
def extract_dbo_gross_surplus(token, schema, table_name, target_source_path: str, target_sheet: str, **kwargs):
    """
    """
    
    # motherduck connect
    con = duckdb.connect(f'md:?motherduck_token={token}')
    con.sql("use asha_production;")
    
    load_date = datetime.now()
    for root, dirs, files in os.walk(target_source_path):
        for file in files:
            if file == "PBI DATA - Copy.xlsx":
                file_path = os.path.join(root, file)
                with pd.ExcelFile(file_path) as xls:
                    for sheet in xls.sheet_names:
                        if sheet == target_sheet:
                            df = pd.read_excel(xls, sheet_name=sheet)
                            
                            # remove spaces from columns
                            df.columns = [col.replace(" ", "") for col in df.columns]
             
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            expected_schema = [
                                "Cycle",
                                "Surplus",
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                                "Cycle": 'VARCHAR(10)',
                                "Surplus": "FLOAT"
                            }
                                                                 
                            # add load date
                            expected_schema.append('LoadDate')
                            column_data_types['LoadDate'] = 'TIMESTAMP'
                            
                            # apply the schema
                            df = df[expected_schema]
                            
                            # write to motherduck
                            con.sql(f"create or replace table {schema}.{table_name} as select * from df;")
                            con.close()
                                
if __name__ == "__main__":
    
    # get motherduck token
    server_config = "/home/asha/airflow/duckdb-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)
    token = config['token']
    
    # source target config
    source_config = "/home/asha/airflow/target-source-config.json"
    with open(source_config, "r") as fp:
        src_config = json.load(fp)
        
    # prepare details to connect
    target_source_path = src_config.get("target_source_path")
    
    # this is the ETL task
    schema = "bronze"
    table_name = 'dbo_gross_surplus'
    target_sheet = 'Gross Surplus'

    extract_dbo_gross_surplus(
        token=token,
        schema=schema,
        table_name=table_name,
        target_source_path=target_source_path,
        target_sheet=target_sheet
    )