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
def extract_dbo_property_database_data(token, schema, table_name, target_source_path: str, target_sheet: str, **kwargs):
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

                            # clean operations
                            df.columns = [col.replace(" ", "") for col in df.columns]
                            df = df.rename(columns={"Provider": "SupportProviders"})

                            # filter grand total and drop rows with blank providers
                            df = df.loc[df["PropertyAddress"] != "Total"]
                            df = df.dropna(subset=['PropertyAddress'])
                            
                            # clean operations
                            df['SupportProviders'] = df['SupportProviders'].str.upper() # AMBER HOUSING
                            df['SupportProviders'] = df['SupportProviders'].apply(lambda x: lookup_support_provider(x))
                            df['PropertyAddress'] = df['PropertyAddress'].str.upper()
                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            expected_schema = [
                                "PropertyAddress",
                                "Rooms",
                                "PropertyCount",
                                "SupportProviders",
                                "CouncilTaxBand",
                                "PropertyUsage",
                                "OtherSpecify",
                                "LoadDate"
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                                "PropertyAddress": "VARCHAR(255)",
                                "Rooms": "INT",
                                "PropertyCount": "INT",
                                "SupportProviders": "VARCHAR(100)",
                                "CouncilTaxBand": "VARCHAR(100)",
                                "PropertyUsage": "VARCHAR(255)",
                                "OtherSpecify": "VARCHAR(255)",
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
    table_name = 'dbo_property_database'
    target_sheet = 'Master Property Database'

    extract_dbo_property_database_data(
        token=token,
        schema=schema,
        table_name=table_name,
        target_source_path=target_source_path,
        target_sheet=target_sheet
    )