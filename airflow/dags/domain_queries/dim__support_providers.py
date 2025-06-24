# packages
import json
import pandas as pd
import numpy as np
import mysql.connector

def create_domain_support_providers_table(host, user, root_pass, domain_table, **kwargs) -> None:
    
    # Establish connection to domain
    domain_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='domain'
        )
  
    support_provider_config = 'airflow/support-providers-config.json'
    
    # read into dataframe
    df = pd.read_json(support_provider_config)
    
    # sort df
    df = df.sort_values(by=['housing_providers'], ascending=[True])
    df['Order'] = range(1, len(df) + 1)
    df.columns = ["SupportProviders", "Order"]
    
    # establish df order
    df = df[["SupportProviders", "Order"]]
    
    # esablish connection to domain
    domain_cursor = domain_db.cursor()
    
    column_data_types = {
        "SupportProviders": "VARCHAR(100)",
        "`Order`" : "INT NOT NULL"
    }
    
    column_headers = [f"{col} {data_type}" for col, data_type in column_data_types.items()]
    column_headers_string = ", ".join(column_headers)
    print(column_headers_string)
    
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
        insert_query = f"insert into {domain_table} values ({values_string});"
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
    domain_table = 'dim__support_providers'
    
    create_domain_support_providers_table(host, user, root_pass, domain_table)