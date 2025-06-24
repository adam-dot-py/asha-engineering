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
import mysql.connector
from pathlib import Path
from mysql.connector import Error
from datetime import datetime

def get_closest_match(value):
    
    # import server config file
    provider_config = "/home/asha/airflow/support-providers-config.json"

    with open(provider_config, "r") as fp:
      config = json.load(fp)
    
    # Extract the list of housing providers
    provider_list = config['housing_providers']
    
    # Find the closest match in the provider list
    matches = difflib.get_close_matches(value, provider_list, n=1, cutoff=0.95)  # Adjust cutoff as needed
    return matches[0] if matches else value  # Return the closest match or the original value if no match

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
def extract_dbo_dal_properties_data(host, user, root_pass, db_name, table_name, target_source_path: str, target_sheet: str, **kwargs):
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
                            
                            # remove spaces from columns
                            df = df.rename(columns={"Is the Landlord same as Company Director? (Yes or No)": "isLandlordSameAsCompanyDirector"})
                            df.columns = [col.replace(" ", "") for col in df.columns]
                            
                            # filter grand total and drop rows with blank providers
                            df = df.loc[df["SupportProviders"] != "Grand Total"]
                            df = df.dropna(subset=['SupportProviders'])

                            # clean operations
                            df['SupportProviders'] = df['SupportProviders'].str.upper() # AMBER HOUSING
                            df['SupportProviders'] = df['SupportProviders'].apply(lambda x: lookup_support_provider(x))
                            
                            # replace Yes with 1 or 0
                            df['isLandlordSameAsCompanyDirector'] = df['isLandlordSameAsCompanyDirector'].replace({"Yes": 1, "No": 0})

                            # replace nan
                            df = df.fillna(0)
                                 
                            # define table structure
                            expected_schema = [
                                "SupportProviders",
                                "PropertyAddress"
                            ]
                            
                            # define table datatypes
                            column_data_types = {
                                "SupportProviders": 'VARCHAR(255)',
                                "PropertyAddress": 'VARCHAR(255)'
                            }
                            
                            # compile the data types and schema
                            for col in df.columns:
                                if (col not in expected_schema) and (col != "isLandlordSameAsCompanyDirector"):
                                    column_data_types[col] = 'INTEGER'
                                    expected_schema.append(col)
                                if col == 'isLandlordSameAsCompanyDirector':
                                    column_data_types[col] = 'BOOLEAN'
                                    expected_schema.append(col)
                                                       
                            # # add load date
                            df['LoadDate'] = load_date
                            column_data_types['LoadDate'] = 'TIMESTAMP'
                            expected_schema.append('LoadDate')
                            
                            # apply the schema
                            df = df[expected_schema]
                 
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
    table_name = 'dbo_dal_properties'
    target_sheet = 'Properties Under SP with DAL'

    extract_dbo_dal_properties_data(host=host, user=user, root_pass=root_pass, db_name=db_name, table_name=table_name, target_source_path=target_source_path, target_sheet=target_sheet)