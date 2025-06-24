# packages
import mysql.connector
import pandas as pd
import numpy as np
import os
import json

def create_fact_tsm_comments(host, user, root_pass, base_table, domain_table):
    """__summary__

    Args:
    __args__
    
    """
    
    # Establish connection to base
    base_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='base'
        )
    
    # Establish connection to domain
    domain_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='domain'
        )
    
    # create a base cursor
    base_cursor = base_db.cursor()
    
    # create the table again
    target_columns = [
        'IDSK',
        'ID', 
        'ASHA_COMMENT'
    ]
    
    base_query = f"select {', '.join(target_columns)} from {base_table} where `ASHA_COMMENT` is not null;"
    base_cursor.execute(base_query)
    
    # Fetch all the results
    base_result = base_cursor.fetchall()
 
    # Convert the result to a dataframe
    df = pd.DataFrame(base_result, columns=base_cursor.column_names)
    
    # Close the connections
    base_cursor.close()
    base_db.close()
    
    # establish a connection to domain
    domain_cursor = domain_db.cursor()
    
    column_data_types = {
        "IDSK": "VARCHAR(30)",
        "ID" : "INT NOT NULL",
        "ASHA_COMMENT" : "TEXT"
    }

if __name__ == '__main__':
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    base_table = 'tsm_responses'
    domain_table = 'fact__tsm_comments'
    
    create_fact_tsm_comments(host, user, root_pass, base_table, domain_table)