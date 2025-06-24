# packages
import pandas as pd
import numpy as np
import json
import time
import mysql.connector

def log_execution(func):
    """logs the execution time of a function
    """
    
    def etl_task_time(*args, **kwargs):
        start_time = time.time()
        print(f"Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        print(f"Finished '{func.__name__}' in {time.time() - start_time} seconds.")
        return result

    return etl_task_time

@log_execution
def create_domain_tsm_scale_responses_table(host, user, root_pass, domain_table, **kwargs) -> None:
    
    # Establish connection to domain
    domain_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='domain'
        )
    
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
    
    # esablish connection to domain
    domain_cursor = domain_db.cursor()
    
    column_data_types = {
        "Response_SK" : "int AUTO_INCREMENT primary key not null",
        "Scale_SK" : "int not null",
        "ResponseText" : "VARCHAR(50)",
        "ResponseWeight" : "int not null",
        "Sentiment" : "VARCHAR(20)"
    }
    
    column_headers = [f"{col} {data_type}" for col, data_type in column_data_types.items()]
    column_headers_string = ", ".join(column_headers)
    
    # drop the table if it exists already, then recreate it
    try:
        print(f"Dropping existing -> {domain_table}")
        drop_query = f"drop table {domain_table};"
        domain_cursor.execute(drop_query)
        print(f"Dropped -> {domain_table}")
        
    except Exception as e:
        print(f"Failed -> {e}")
        
    finally:
        # create the table
        create_query = f"create table if not exists {domain_table} ({column_headers_string});"
        domain_cursor.execute(create_query)
        print(f"Created -> {domain_table}")
        
    # insert the data
    row_count = 0
    for _, row in df.iterrows():
        values = []
        for value in row:
            values.append(f"'{value}'")
            row_count += 1
    
        values_string = ", ".join(values)
        insert_query = f"insert into {domain_table} (Scale_SK, ResponseText, ResponseWeight, Sentiment) values ({values_string});"
        domain_cursor.execute(insert_query)
    
    # commit changes and close connections
    domain_db.commit()
    domain_cursor.close()
    domain_db.close()

if __name__ == '__main__':
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    domain_table = 'dim__tsm_scale_responses'

    create_domain_tsm_scale_responses_table(host, user, root_pass, domain_table)