# packages
import duckdb
import json
import time
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
def transform_dbo_rc_ratio_data(bronze_schema, gold_schema, bronze_table_name, gold_table_name, con, **kwargs):
    """
    """
    
    # connect to motherduck
    con.sql("USE asha_production;")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema};")
    
    load_date = datetime.now()
     
    # Convert the result to a dataframe
    bronze_df = con.sql(f"SELECT * FROM {bronze_schema}.{bronze_table_name};").df()
    
    # drop the base load date
    bronze_df = bronze_df.drop(columns='LoadDate')
    
    # melt the dataframe
    mdf = pd.melt(
        bronze_df,
        id_vars=["SupportProviders"],
        var_name="Cycle",
        value_name="Value",
    )
    
    # add load date
    mdf['LoadDate'] = load_date
    
    expected_schema = [
        "SupportProviders",
        "Cycle",
        "Value",
        "LoadDate"
    ]
    
    column_data_types = {
        "SupportProviders": 'VARCHAR(255)',
        "Cycle": 'VARCHAR(10)',
        "Value": 'FLOAT',
        "LoadDate": 'TIMESTAMP'
    }
    
    # write to motherduck
    con.sql(f"CREATE OR REPLACE TABLE {gold_schema}.{gold_table_name} AS SELECT * FROM mdf;")
    con.close()
                                
if __name__ == "__main__":
    
    # this is the ETL task
    bronze_schema = 'bronze'
    gold_schema = 'gold'
    bronze_table_name = 'dbo_rc_ratio_data'
    gold_table_name = 'fact__rc_data'

    transform_dbo_rc_ratio_data(
        bronze_schema=bronze_schema,
        gold_schema=gold_schema,
        bronze_table_name=bronze_table_name,
        gold_table_name=gold_table_name
    )