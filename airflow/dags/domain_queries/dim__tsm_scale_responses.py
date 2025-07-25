# packages
import duckdb
import pandas as pd
import json
import time
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
def create_domain_tsm_scale_responses_table(schema, domain_table, con, **kwargs) -> None:
    
    # connect to motherduck
    con.sql("USE asha_production;")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")

    # ResponseText, Scale_SK, ResponseSK, ResponseWeight, Sentiment
    #TODO: Add sentiment
    domain_dict = {
        '1' : [2, 1, 1, "Detractor"],
        '2' : [2, 2, 2, "Detractor"],
        '3' : [2, 3, 3, "Detractor"],
        '4' : [2, 4, 4, "Detractor"],
        '5' : [2, 5, 5, "Detractor"],
        '6' : [2, 6, 6, "Detractor"],
        '7' : [2, 7, 7, "Passive"],
        '8' : [2, 8, 8, "Passive"],
         9 : [2, 9, 9, "Promoter"],
        '10' : [2, 10, 10, "Promoter"],
        'very satisfied' : [1, 11, 1, "Positive"],
        'fairly satisfied' : [1, 12, 2, "Positive"],
        'neither satisfied nor dissatisfied' : [1, 13, 3, "Neutral"],
        'fairly dissatisfied' : [1, 14, 4, "Negative"],
        'very dissatisfied' : [1, 15, 5, "Negative"],
        'not applicable/ do not know' : [1, 16, 0, "Na"],
        'strongly agree' : [1, 17, 1, "Positive"],
        'agree' : [1, 18, 2, "Positive"],
        'neither agree nor disagree': [1, 19, 3, "Neutral"],
        'disagree' : [1, 20, 4, "Negative"],
        'strongly disagree' : [1, 21, 5, "Negative"],
        'yes' : [3, 22, 1, "NA"],
        'no' : [3, 23, 0, "NA"],
        'do not know' : [3, 24, 0, "NA"],
        'None' : [3,25,0,"NA"],
        '0' : [2,26,0,"Detractor"]
    }

    df = pd.DataFrame.from_dict(domain_dict, orient='index').reset_index()

    columns = [
        "ResponseText",
        "Scale_SK",
        "Response_SK",
        "ResponseWeight",
        "Sentiment"
    ]

    df.columns = columns

    # reorder
    df = df[[
        "Scale_SK",
        "ResponseText",
        "ResponseWeight",
        "Sentiment"
    ]]
    
    con.sql(f"CREATE OR REPLACE TABLE {schema}.{domain_table} AS SELECT * FROM df;")
    con.close()

if __name__ == '__main__':
    
    schema = 'silver'
    domain_table = 'dim__tsm_scale_responses'

    create_domain_tsm_scale_responses_table(schema, domain_table)