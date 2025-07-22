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
import numpy as np
import mysql.connector
from pathlib import Path
from mysql.connector import Error
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
def extract_dbo_leavers_data(token, schema, table_name, target_source_path: str, target_sheet: str, **kwargs):
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
                            df = pd.read_excel(xls, sheet_name=sheet, na_values=['nan', '', ' '])
                            
                            # clean operations
                            df.columns = [col.replace(" ", "") for col in df.columns] # remove spaces
                            df.columns = [re.sub('[^A-Za-z0-9]+', '', col) for col in df.columns] # remove special characters
                            df['TenantName'] = df['TenantName'].str.upper()
                            df['TenantName'] = df['TenantName'].str.strip()
                            df['LeavingAddress'] = df['LeavingAddress'].str.upper()
                            df['LeavingAddress'] = df['LeavingAddress'].str.strip()
                            
                            # coerce dates
                            df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce').dt.date
                            df['VacateDate'] = pd.to_datetime(df['VacateDate'], errors='coerce').dt.date
                            df['DateInformed'] = pd.to_datetime(df['DateInformed'], errors='coerce').dt.date

                            # filtering
                            df = df.dropna(subset=['SrNo'])
                            
                            # protect pii
                            df['NINumber'] = df['NINumber'].str[:2] + '******'
                            df['CRN'] = '****' + df['CRN'].str[-4:]
                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            expected_schema = [
                              "SrNo",
                              "TenantName",
                              "CRN",
                              "LeavingAddress",
                              "NINumber",
                              "DOB",
                              "VacateDate",
                              "DateInformed",
                              "Notes",
                              "LoadDate"
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                              "SrNo": "INT",
                              "TenantName": "VARCHAR(255)",
                              "CRN": "VARCHAR(255)",
                              "LeavingAddress": "VARCHAR(255)",
                              "NINumber": "VARCHAR(100)",
                              "DOB": "DATE",
                              "VacateDate": "DATE",
                              "DateInformed": "DATE",
                              "Notes": "TEXT",
                              "LoadDate": 'TIMESTAMP'
                            }

                            # apply the schema
                            df = df[expected_schema]
                            
                            # write to motherduck
                            con.sql(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df;")
                            con.close()

                                
if __name__ == "__main__":
    
    # import motherduck token and target source config
    target_source_config = "/home/asha/airflow/target-source-config.json"
    server_config = "/home/asha/airflow/duckdb-config.json"
        
    with open(target_source_config, "r") as t_con:
        target_config = json.load(t_con)

    with open(server_config, "r") as fp:
        config = json.load(fp)
    token = config['token']
    
    # this is the ETL task
    target_source_path = target_config.get("target_source_path")
    schema = "bronze"
    table_name = 'dbo_leavers'
    target_sheet = 'Leavers'

    extract_dbo_leavers_data(
        token=token,
        schema=schema,
        table_name=table_name,
        target_source_path=target_source_path,
        target_sheet=target_sheet
    )