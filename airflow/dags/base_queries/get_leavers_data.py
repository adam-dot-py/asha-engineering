# append custom function path and bring in lookup
import sys
sys.path.append('/home/asha/airflow/dags/custom_functions')
from lookup_support_provider import lookup_support_provider

# packages
import os
import json
import time
import re
import pandas as pd
import numpy as np
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
def extract_dbo_leavers_data(host, user, root_pass, db_name, table_name, target_source_path: str, target_sheet: str, **kwargs):
    """
    """

    mysqlconnection = server_connection(host, user, root_pass, db_name)
    load_date = datetime.now()
    for root, dirs, files in os.walk(target_source_path):
        for file in files:
            if file == "PBI DATA.xlsx":
                file_path = os.path.join(root, file)
                with pd.ExcelFile(file_path) as xls:
                    for sheet in xls.sheet_names:
                        if sheet == target_sheet:
                            df = pd.read_excel(xls, sheet_name=sheet, na_values=['nan', '', ' '])
                            
                            # clean operations
                            df.columns = [col.replace(" ", "") for col in df.columns] # remove spaces
                            df.columns = [re.sub('[^A-Za-z0-9]+', '', col) for col in df.columns] # remove special characters
                            df['TenantName'] = df['TenantName'].str.upper()
                            df['TenantName'] = df['TenantName'].str.strip()
                            df['LeavingAddress'] = df['LeavingAddress'].str.upper()
                            df['LeavingAddress'] = df['LeavingAddress'].str.strip()
                            
                            # coerce dates
                            df['DOB'] = pd.to_datetime(df['DOB'], errors='coerce')
                            df['VacateDate'] = pd.to_datetime(df['VacateDate'], errors='coerce')
                            df['DateInformed'] = pd.to_datetime(df['DateInformed'], errors='coerce')

                            # filtering
                            df = df.dropna(subset=['SrNo'])
                            
                            # protect pii
                            df['NINumber'] = df['NINumber'].str[:2] + '******'
                            df['CRN'] = '****' + df['CRN'].str[-4:]
                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            expected_schema = [
                              "SrNo",
                              "TenantName",
                              "CRN",
                              "LeavingAddress",
                              "NINumber",
                              "DOB",
                              "VacateDate",
                              "DateInformed",
                              "Notes",
                              "LoadDate"
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                              "SrNo": "INT",
                              "TenantName": "VARCHAR(255)",
                              "CRN": "VARCHAR(255)",
                              "LeavingAddress": "VARCHAR(255)",
                              "NINumber": "VARCHAR(100)",
                              "DOB": "DATE",
                              "VacateDate": "DATE",
                              "DateInformed": "DATE",
                              "Notes": "TEXT",
                              "LoadDate": 'TIMESTAMP'
                            }

                            # apply the schema
                            df = df[expected_schema]
                            print(df.head(1))
                 
                            # create the table
                            # this will create a string which is passed into the SQL query. We take the column header and its respective data type as defined above.
                            column_headers = [f"`{col}` {data_type}" for col, data_type in column_data_types.items()]
                            
                            # looks like this column_headers.append('PRIMARY KEY (ID)')
                            column_headers_string = ', '.join(column_headers)
                            
                            # establish cursor connection
                            mycursor = mysqlconnection.cursor()
                            
                            # drop the table
                            try:
                                drop_query = "DROP TABLE {db_name}.{table_name}".format(db_name=db_name, table_name=table_name)
                                mycursor.execute(drop_query)
                                print(f"Dropped -> {db_name}.{table_name}")
                            except:
                                print(f"Does not exist yet -> {db_name}.{table_name}")
                            
                            # create the table again
                            try:
                                create_table_query = "CREATE TABLE IF NOT EXISTS {table_name} ({column_headers_string})".format(table_name = table_name, column_headers_string = column_headers_string)
                                print(f"Create query -> {create_table_query}")
                                mycursor.execute(create_table_query)
                                print(f"Recreated -> {table_name}")
                            except Exception as e:
                                print(e)
                                
                            # add data to database
                            for _, row in df.iterrows():
                                
                                # generate insert statement
                                values = []
                                for value in row:
                                    if pd.isnull(value) or value == '' or value == 'nan' or value == 'NaN':
                                        # Handle NaN, empty string, or the string 'nan' as NULL
                                        values.append('NULL')
                                    elif isinstance(value, (int, float)):
                                        # For numeric values, append them directly
                                        values.append(str(value))
                                    elif isinstance(value, str):
                                        # For string values, clean them and wrap them in single quotes
                                        value = value.strip()  # Remove leading and trailing whitespace
                                        value = re.sub(r"'", r"", value)  # Replace apostrophes with nothing
                                        values.append(f"'{value}'")
                                    elif isinstance(value, (pd.Timestamp, datetime)):
                                        # For datetime/timestamp values, wrap them in single quotes
                                        values.append(f"'{value}'")
                                                                    
                                # create the values string, to be passed in the query
                                values_string = ', '.join(values)
                                insert_query = f"insert into {table_name} values ({values_string})"
                                
                                # execute and commit changes
                                try:
                                    mycursor.execute(insert_query)
                                except Exception as e:
                                    print(f"Something went wrong -> {insert_query}")
                                    mysqlconnection.close()
                                    mycursor.close()
                                    raise e
                                
                            mysqlconnection.commit()
                            mycursor.close()
                            mysqlconnection.close()
                            print("MySQL connection is closed")
                                
if __name__ == "__main__":
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    
    # source target config
    source_config = "/home/asha/airflow/target-source-config.json"
    with open(source_config, "r") as fp:
        src_config = json.load(fp)

    # prepare details to connect
    target_source_path = src_config.get("target_source_path")
    
    # this is the ETL task
    db_name = "base"
    table_name = 'dbo_leavers'
    target_sheet = 'Leavers'

    extract_dbo_leavers_data(host=host, user=user, root_pass=root_pass, db_name=db_name, table_name=table_name, target_source_path=target_source_path, target_sheet=target_sheet)