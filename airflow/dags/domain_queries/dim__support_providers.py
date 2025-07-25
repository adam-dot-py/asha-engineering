# packages
import duckdb
import time
import json
import pandas as pd
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
def create_domain_support_providers_table(schema, domain_table, con, **kwargs) -> None:
    
    # connect with motherduck
    con.sql("USE asha_production;")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    
    # read into dataframe
    support_provider_config = 'airflow/support-providers-config.json'
    df = pd.read_json(support_provider_config)
    
    # sort df
    df = df.sort_values(by=['housing_providers'], ascending=[True])
    df['Order'] = range(1, len(df) + 1)
    df.columns = ["SupportProviders", "Order"]
    
    # establish df order
    df = df[["SupportProviders", "Order"]]
    
    # write to motherduck
    con.sql(f"CREATE OR REPLACE TABLE {schema}.{domain_table} AS SELECT * FROM df;")
    con.close()

if __name__ == '__main__':
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    schema = 'silver'
    domain_table = 'dim__support_providers'
    
    create_domain_support_providers_table(schema, domain_table)