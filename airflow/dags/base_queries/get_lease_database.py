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
def extract_dbo_lease_database(token, schema, table_name, target_source_path: str, target_sheet: str, **kwargs):
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
                            df.columns = [col.replace(" ", "") for col in df.columns] # remove spaces
                            df.columns = [re.sub('[^A-Za-z0-9]+', '', col) for col in df.columns] # remove special characters
                            df = df.loc[df["PropertyAddress"] != "Total"]
                            
                            # formatting
                            df['SupportProviders'] = df['SupportProviders'].str.upper()
                            df['SupportProviders'] = df['SupportProviders'].apply(lambda x: lookup_support_provider(x))
                            df['PropertyAddress'] = df['PropertyAddress'].str.upper()
                                                       
                            # coerce dates
                            df['LeaseStartDate'] = pd.to_datetime(df['LeaseStartDate'], errors='coerce')
                            df['LeaseEndDate'] = pd.to_datetime(df['LeaseEndDate'], errors='coerce')
                            df['RenewedLeaseStartDate'] = pd.to_datetime(df['RenewedLeaseStartDate'], errors='coerce')
                            df['RenewedLeaseEndDate'] = pd.to_datetime(df['RenewedLeaseEndDate'], errors='coerce')
                            
                            complex_data_cols = [
                                'ChangeofRS01062023RenewedLeaseStartDate',
                                'ChangeofRS01062023RenewedLeaseEndDate',
                                'ChangeofRS02122024RenewedLeaseStartDate',
                                'ChangeofRS02122024RenewedLeaseEndDate'
                            ]
                            
                            for col in complex_data_cols:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                                 
                            # filtering
                            df = df.dropna(subset=['SupportProviders'])
                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            # expected_schema = [df.columns]
                            column_data_types = {}
                            
                            # define table datatypes
                            for col in df.columns:
                                if col in ["PropertyAddress", "SupportProviders", "CouncilTaxBand", "Comments"]:
                                    column_data_types[col] = "VARCHAR(255)"
                                if col in ["Rooms", "PropertyCount"]:
                                    column_data_types[col] = "INT"
                                if col in ["AnnualLeaseCostUpdated02122024", "1monthNoticeCost", "3monthNoticeCost"]:
                                    column_data_types[col] = "FLOAT"
                                if col in ["LoadDate"]:
                                    column_data_types[col] = "TIMESTAMP"
                                if col in complex_data_cols:
                                    column_data_types[col] = "DATE"
                                if col in ["LeaseStartDate", "LeaseEndDate", "RenewedLeaseStartDate", "RenewedLeaseEndDate"]:
                                    column_data_types[col] = "DATE"
                                           
                            # apply the schema
                            # df = df[expected_schema]
                            
                            # write to motherduck
                            con.sql(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df;")
                            con.close()
                            
                                
if __name__ == "__main__":
    
    # import server config file and target source config
    target_source_config = "/home/asha/airflow/target-source-config.json"
        
    with open(target_source_config, "r") as t_con:
        target_config = json.load(t_con)

    # get motherduck token
    server_config = "/home/asha/airflow/duckdb-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)
    token = config['token']
    
    # this is the ETL task
    target_source_path = target_config.get("target_source_path")
    schema = "bronze"
    table_name = 'dbo_lease_database'
    target_sheet = 'Lease Database'

    extract_dbo_lease_database(token=token, schema=schema, table_name=table_name, target_source_path=target_source_path, target_sheet=target_sheet)