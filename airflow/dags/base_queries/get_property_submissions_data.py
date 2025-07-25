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
from datetime import datetime
from functools import wraps

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
    """_docstring
    """
    
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
def extract_dbo_property_data(token, schema, table_name, target_source_path: str, target_sheet: str, con, **kwargs):
    """
    """

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

                            # clean operations
                            df.columns = [col.replace(" ", "") for col in df.columns]

                            # filter grand total and drop rows with blank providers
                            df = df.loc[df["SupportProviders"] != "Grand Total"]
                            df = df.dropna(subset=['SupportProviders'])
                            
                            # capitalise support providers
                            df['SupportProviders'] = df['SupportProviders'].str.upper()
                            df['SupportProviders'] = df['SupportProviders'].apply(lambda x: lookup_support_provider(x))
                            
                            # replace self-contained flat with 1
                            df['Units'] = df['Units'].replace('Self-Contained Flat', '1')
                            
                            # fix submission date
                            df['SubmissionDate'] = pd.to_datetime(df['SubmissionDate']).dt.date
                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            expected_schema = [
                                "SrNo",
                                "PropertyAddress",
                                "Units",
                                "SupportProviders",
                                "SubmissionDate",
                                "LoadDate"
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                                "SrNo": 'INTEGER',
                                "PropertyAddress": 'VARCHAR(255)',
                                "Units": 'INTEGER',
                                "SupportProviders": 'VARCHAR(255)',
                                "SubmissionDate": 'DATE',
                                "LoadDate": 'TIMESTAMP'
                            }

                            # apply the schema
                            df = df[expected_schema]
                            
                            # write to motherduck
                            con.sql(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df;")
                            con.close()
                                
if __name__ == "__main__":
    
    # this is the ETL task
    target_source_path = target_config.get("target_source_path")
    schema = 'bronze'
    table_name = 'dbo_property_submissions'
    target_sheet = 'Property Sub'

    extract_dbo_property_data(
        token=token,
        schema=schema, 
        table_name=table_name, 
        target_source_path=target_source_path, 
        target_sheet=target_sheet
    )