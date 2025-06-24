# packages
import pandas as pd
import numpy as np
import mysql.connector
import json

def replace_text(text):
    """Escapes single quotes and handles other special characters."""
    if isinstance(text, str):
        return text.replace("'", "''")  # Double single quotes for MySQL
    return text


def create_fact_tenant_records(host, user, root_pass, base_table, domain_table) -> None:
    """_docstring
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
    base_query = f"select * from {base_table} order by CycleNumberValue desc, Tenant_SK asc;"
    base_cursor.execute(base_query)
    
    # Fetch all the results
    base_result = base_cursor.fetchall()

    # Convert the result to a dataframe
    df = pd.DataFrame(base_result, columns=base_cursor.column_names)
    
    # Close the connections
    base_cursor.close()
    base_db.close()
    
    # Use groupby + transform to get the latest known value for each tenant
    helper_columns = []
    target_columns = ['SexualOrientation', 'ReferralAgency', 'GroupedReferralAgency', 'Age', 'Gender', 'Religion', 'Ethnicity', 'SpokenLanguage', 'Nationality', 'Disability', 'LengthOfStay', 'RiskAssessment']
    isDifferent_columns = []
    last_columns = []
    for col in target_columns:
        helper_name = f"latest_{col}"
        last_name = f"last_{col}"
        isDifferent_name = f'isDifferent_{col}'
        
        # get the latest value and first ever values
        df[helper_name] = df.groupby('Tenant_SK')[col].transform('first')
        df[last_name] = df.groupby('Tenant_SK')[col].transform('last')
        
        # update the target column with the latest value
        df[col] = df[helper_name]
        
        # track if different per target column
        df[isDifferent_name] = (df[helper_name] != df[last_name]).astype(int)
        helper_columns.append(helper_name)
        isDifferent_columns.append(isDifferent_name)
        last_columns.append(last_name)
    
    # in this section we want to check if any of what should be static values have changed between latest and first
    check_columns = ['SexualOrientation', 'Gender', 'Religion', 'Ethnicity', 'SpokenLanguage', 'Nationality', 'Disability']
    df['isDifferentOverall'] = df[[f'isDifferent_{col}' for col in check_columns]].any(axis=1).astype(int) # cool
    
    # esablish connection to domain
    domain_cursor = domain_db.cursor()
    
    table_schema = [
        'Tenant_SK',
        'PropertyAddress',
        'Room',
        'FirstName',
        'MiddleName',
        'LastName',
        'DateOfBirth',
        'NINumber',
        'CheckinDate',
        'CheckoutDate',
        'NewHBClaim',
        'HBClaimRefNumber', 
        'ReferralAgency',
        'GroupedReferralAgency',
        'Age',
        'Gender', 
        'Religion',
        'Ethnicity', 
        'Nationality', 
        'Disability',
        'SexualOrientation',
        'SpokenLanguage', 
        'RiskAssessment',
        'LengthOfStay',
        'CycleNumber',
        'CycleNumberValue',
        'Source',
        'ProviderName',
        'isDifferentOverall',
        'isDifferent_SexualOrientation',
        'isDifferent_Gender',
        'isDifferent_Religion',
        'isDifferent_Ethnicity',
        'isDifferent_SpokenLanguage',
        'isDifferent_Nationality',
        'isDifferent_Disability',
        'LoadDate'
    ]
    
    column_data_types = {
        'Tenant_SK' : 'VARCHAR (100)',
        'PropertyAddress' : 'VARCHAR (100)',
        'Room' : 'VARCHAR(30)', 
        'FirstName' : 'VARCHAR(100)', 
        'MiddleName': 'VARCHAR(100)', 
        'LastName': 'VARCHAR(100)',
        'DateOfBirth' : 'VARCHAR(30)', 
        'NINumber' : 'VARCHAR(30)', 
        'CheckinDate' : 'VARCHAR(30)', 
        'CheckoutDate': 'VARCHAR(30)' ,
        'NewHBClaim': 'VARCHAR(30)',
        'HBClaimRefNumber': 'VARCHAR(100)', 
        'ReferralAgency': 'VARCHAR(100)',
        'GroupedReferralAgency' : 'VARCHAR(100)',
        'Age' : "VARCHAR(10)", 
        'Gender' : 'VARCHAR(30)', 
        'Religion' : 'VARCHAR(200)',
        'Ethnicity' : 'VARCHAR(200)', 
        'Nationality' : 'VARCHAR(200)', 
        'Disability' : 'VARCHAR(200)', 
        'SexualOrientation' : 'VARCHAR(100)',
        'SpokenLanguage': 'VARCHAR(100)', 
        'RiskAssessment': 'VARCHAR(100)', 
        'LengthOfStay': 'VARCHAR(100)', 
        'CycleNumber' : 'VARCHAR(100)',
        'CycleNumberValue' : 'INT',
        'Source' : 'VARCHAR(100)',
        'ProviderName' : 'VARCHAR(100)',
        'isDifferentOverall' : 'INT',
        'isDifferent_SexualOrientation' : 'INT',
        'isDifferent_Gender' : 'INT',
        'isDifferent_Religion' : 'INT',
        'isDifferent_Ethnicity' : 'INT',
        'isDifferent_SpokenLanguage' : 'INT',
        'isDifferent_Nationality' : 'INT',
        'isDifferent_Disability' : 'INT',
        'LoadDate': 'DATETIME'
    }

    # align to table schema
    df = df[table_schema]
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
                # sanitize for safe injection using MySQL syntax
                sanitized_value = replace_text(value)
                values.append(f"'{sanitized_value}'")
                row_count += 1
            
            values_string = ", ".join(values)
            insert_query = f"insert into {domain_table} values ({values_string});"
            domain_cursor.execute(insert_query)
        
        # commit changes and close connections
        domain_db.commit()
        domain_cursor.close()
        domain_db.close()
        
        print("Process finished.")

if __name__ == '__main__':
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    base_table = 'tenant_data'
    domain_table = 'fact__tenant_records'

    create_fact_tenant_records(host, user, root_pass, base_table, domain_table)