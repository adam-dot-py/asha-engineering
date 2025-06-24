# packages
import pandas as pd
import time
import json
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
def create_domain_tsm_scales_table(host, user, root_pass, domain_table, **kwargs) -> None:
    
    # Establish connection to domain
    domain_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='domain'
        )
    
    # ResponseText, Scale_SK, ResponseSK, ResponseWeight
    domain_dict = {
        '1' : "Likert",
        '2' : "Net Promoter Score",
        '3' : "Confirmation"
    }

    df = pd.DataFrame.from_dict(domain_dict, orient='index').reset_index()

    columns = [
        "Scale_SK",
        "ScaleName"
    ]

    df.columns = columns
    
    # esablish connection to domain
    domain_cursor = domain_db.cursor()
    
    column_data_types = {
        "Scale_SK" : "int primary key not null",
        "ScaleName" : "VARCHAR(30)"
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
    insert_query = f"insert into {domain_table} (Scale_SK, ScaleName) values ({values_string});"
    domain_cursor.execute(insert_query)
    
    # commit changes and close connections
    domain_db.commit()
    domain_cursor.close()
    domain_db.close()

if __name__ == "__main__":
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    domain_table = 'dim__tsm_scales'

    create_domain_tsm_scales_table(host, user, root_pass, domain_table)