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
def extract_dbo_hqi_data(token, schema, table_name, target_source_path: str, target_sheet: str, **kwargs):
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
                            df = df.rename(columns={"NumberofHousingQualityIssuesResolvedwithinGivenTimeFrame" : "NumberofHousingQualityIssuesResolvedwithinGivenTimeFrame%"})
                            df = df.fillna(0)

                            # filter grand total and drop rows with blank providers
                            df = df.loc[df["SupportProviders"] != "Grand Total"]
                            df = df.dropna(subset=['SupportProviders'])
                            
                            # capitalise support providers
                            df['SupportProviders'] = df['SupportProviders'].str.upper()
                            df['SupportProviders'] = df['SupportProviders'].apply(lambda x: lookup_support_provider(x))                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            expected_schema = [
                                "SupportProviders",
                                "InspectionMonth",
                                "NumberofUnitsCovered",
                                "NumberofHousingQualityIssuesIdentifiedDuringInspection",
                                "HighPriority",
                                "LowPriority",
                                "Non-Emergency(Other)",
                                "NumberofHousingQualityIssuesResolvedwithinGivenTimeFrame%",
                                "LoadDate"
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                                "SupportProviders": 'VARCHAR(255)',
                                "InspectionMonth": 'DATE',
                                "NumberofUnitsCovered": 'INTEGER',
                                "NumberofHousingQualityIssuesIdentifiedDuringInspection": 'INTEGER',
                                "HighPriority": 'INTEGER',
                                "LowPriority": 'INTEGER',
                                "Non-Emergency(Other)": 'INTEGER',
                                "NumberofHousingQualityIssuesResolvedwithinGivenTimeFrame%": 'FLOAT',
                                "LoadDate": 'TIMESTAMP'
                            }
                            
                            # apply the schema
                            df = df[expected_schema]
                            
                            # write to motherduck
                            con.sql(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df;")
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
    table_name = 'dbo_hqi'
    target_sheet = 'HQIs'

    extract_dbo_hqi_data(
        token=token, 
        schema=schema,
        table_name=table_name,
        target_source_path=target_source_path,
        target_sheet=target_sheet
    )