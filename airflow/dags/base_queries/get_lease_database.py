# append custom function path and bring in lookup
import sys
sys.path.append('/home/asha/airflow/dags/custom_functions')
from lookup_support_provider import lookup_support_provider

# packages
import os
import json
import time
import re
import difflib
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
def extract_dbo_lease_database(host, user, root_pass, db_name, table_name, target_source_path: str, target_sheet: str, **kwargs):
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
                            df = pd.read_excel(xls, sheet_name=sheet)
                            
                            # clean operations
                            df.columns = [col.replace(" ", "") for col in df.columns] # remove spaces
                            df.columns = [re.sub('[^A-Za-z0-9]+', '', col) for col in df.columns] # remove special characters
                            df = df.loc[df["PropertyAddress"] != "Total"]
                            
                            # formatting
                            df['SupportProviders'] = df['SupportProviders'].str.upper()
                            df['SupportProviders'] = df['SupportProviders'].apply(lambda x: lookup_support_provider(x)) 
                                                       
                            # coerce dates
                            df['LeaseStartDate'] = pd.to_datetime(df['LeaseStartDate'], errors='coerce')
                            df['LeaseEndDate'] = pd.to_datetime(df['LeaseEndDate'], errors='coerce')
                            df['RenewedLeaseStartDate'] = pd.to_datetime(df['RenewedLeaseStartDate'], errors='coerce')
                            df['RenewedLeaseEndDate'] = pd.to_datetime(df['RenewedLeaseEndDate'], errors='coerce')
                            
                            complex_data_cols = [
                                'ChangeofRS01062023RenewedLeaseStartDate',
                                'ChangeofRS01062023RenewedLeaseEndDate',
                                'ChangeofRS02122024RenewedLeaseStartDate',
                                'ChangeofRS02122024RenewedLeaseEndDate'
                            ]
                            
                            for col in complex_data_cols:
                                df[col] = pd.to_datetime(df[col], errors='coerce')
                                 
                            # filtering
                            df = df.dropna(subset=['SupportProviders'])
                            
                            # add load date
                            df['LoadDate'] = load_date
                            
                            # define table structure
                            # expected_schema = [df.columns]
                            column_data_types = {}
                            
                            # define table datatypes
                            for col in df.columns:
                                if col in ["PropertyAddress", "SupportProviders", "CouncilTaxBand", "Comments"]:
                                    column_data_types[col] = "VARCHAR(255)"
                                if col in ["Rooms", "PropertyCount"]:
                                    column_data_types[col] = "INT"
                                if col in ["AnnualLeaseCostUpdated02122024", "1monthNoticeCost", "3monthNoticeCost"]:
                                    column_data_types[col] = "FLOAT"
                                if col in ["LoadDate"]:
                                    column_data_types[col] = "TIMESTAMP"
                                if col in complex_data_cols:
                                    column_data_types[col] = "DATE"
                                if col in ["LeaseStartDate", "LeaseEndDate", "RenewedLeaseStartDate", "RenewedLeaseEndDate"]:
                                    column_data_types[col] = "DATE"
                                           
                            # apply the schema
                            # df = df[expected_schema]
                 
                            # create the table
                            # this will create a string which is passed into the SQL query. We take the column header and its respective data type as defined above.
                            column_headers = [f"`{col}` {data_type}" for col, data_type in column_data_types.items()]
                            
                            # looks like this column_headers.append('PRIMARY KEY (ID)')
                            column_headers_string = ', '.join(column_headers)
                            
                            # establish cursor connection
                            mycursor = mysqlconnection.cursor()
                            
                            print(column_headers)
                            
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
    
    # import server config file and target source config
    server_config = "/home/asha/airflow/server-config.json"
    target_source_config = "/home/asha/airflow/target-source-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)
        
    with open(target_source_config, "r") as t_con:
        target_config = json.load(t_con)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    
    # this is the ETL task
    target_source_path = target_config.get("target_source_path")
    db_name = "base"
    table_name = 'dbo_lease_database'
    target_sheet = 'Lease Database'

    extract_dbo_lease_database(host=host, user=user, root_pass=root_pass, db_name=db_name, table_name=table_name, target_source_path=target_source_path, target_sheet=target_sheet)