# packages
import os
import time
import re
import pandas as pd
import mysql.connector
from pathlib import Path
from mysql.connector import Error
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

def server_connection(host, user, root_pass, db_name):
    """
    """
    
    try:
        # Establish the MySQL connection
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database=db_name
        )
        
        if connection.is_connected():
            print(f"Connected to {db_name}")
        
        return connection
    
    except Exception as e:
        print(f"Connection failed -> {e}")
        return None

@log_execution
def transform_dbo_clawbacks_data(host, user, root_pass, base_table_name, domain_table_name, **kwargs):
    """
    """
    
    base_mysqlconnection = server_connection(host=host, user=user, root_pass=root_pass, db_name='base')
    domain_mysqlconnection = server_connection(host=host, user=user, root_pass=root_pass, db_name='domain')
    load_date = datetime.now()
    
    base_cursor = base_mysqlconnection.cursor()
    base_query = f"select * from base.{base_table_name}"
    base_cursor.execute(base_query)
    
    # Fetch all the results from the table
    base_result = base_cursor.fetchall()

    # Close the connections
    base_mysqlconnection.close()
    
    # Convert the result to a dataframe
    df = pd.DataFrame(base_result, columns=base_cursor.column_names)
    
    # drop the base load date
    df = df.drop(columns='LoadDate')
    
    # melt the dataframe
    mdf = pd.melt(
        df,
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
                 
    # create the table
    # this will create a string which is passed into the SQL query. We take the column header and its respective data type as defined above.
    column_headers = [f"`{col}` {data_type}" for col, data_type in column_data_types.items()]
    
    # looks like this column_headers.append('PRIMARY KEY (ID)')
    column_headers_string = ', '.join(column_headers)
    
    # establish cursor connection
    domain_cursor = domain_mysqlconnection.cursor()
    
    # drop the table
    try:
        drop_query = f"DROP TABLE domain.{domain_table_name}"
        domain_cursor.execute(drop_query)
        print(f"Dropped -> domain.{domain_table_name}")
    except:
        print(f"Does not exist yet -> domain.{domain_table_name}")
    
    # create the table again
    try:
        create_table_query = "CREATE TABLE IF NOT EXISTS {domain_table_name} ({column_headers_string})".format(domain_table_name=domain_table_name, column_headers_string=column_headers_string)
        print(f"Create query -> {create_table_query}")
        domain_cursor.execute(create_table_query)
        print(f"Recreated -> {domain_table_name}")
    except Exception as e:
        print(e)
        
    # add data to database
    for _, row in mdf.iterrows():
        
        # generate insert statement
        values = []
        for value in row:
            if isinstance(value, int):
                values.append(f"'{value}'")
            elif isinstance(value, float):
                values.append(f"'{value}'")
            elif pd.isnull(value):
                values.append('NULL')
            elif isinstance(value, str):
                value = value.strip()  # Remove leading and trailing whitespace
                value = re.sub(r"'", r"", value) # replace apostrophre's with nothing
                values.append(f"'{value}'")
            elif isinstance(value, pd.Timestamp):
                values.append(f"'{value}'")
                
        # create the values string, to be passed in the query
        values_string = ', '.join(values)
        insert_query = f"insert into {domain_table_name} values ({values_string})"
        
        # execute and commit changes
        try:
            domain_cursor.execute(insert_query)
        except Exception as e:
            print(f"Something went wrong -> {insert_query}")
            domain_cursor.close()
            domain_mysqlconnection.close()
            raise e
        
    domain_mysqlconnection.commit()
    domain_cursor.close()
    domain_mysqlconnection.close()
    print("MySQL connection is closed")
                                
if __name__ == "__main__":
    
    # this is the ETL task
    host = "localhost"
    user = "root"
    root_pass = "admin"
    base_table_name = 'dbo_clawbacks'
    domain_table_name = 'fact__clawbacks'

    transform_dbo_clawbacks_data(host=host, user=user, root_pass=root_pass, base_table_name=base_table_name, domain_table_name=domain_table_name)