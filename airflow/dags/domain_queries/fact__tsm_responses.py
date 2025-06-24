# packages
import mysql.connector
import pandas as pd
import numpy as np
import os
import json

def create_fact_tsm_responses(host, user, root_pass, base_table, domain_table):
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
    
    # create a base cursor
    base_cursor = base_db.cursor()
    
    # create the table again
    target_columns = [
        'IDSK',
        'ID', 
        'EmailAddress', 
        'StartTime',
        'TP01',
        'TP02_TP03_confirm',
        'TP02',
        'TP03',
        'TP04',
        'TP05',
        'TP06',
        'TP07',
        'TP08',
        'TP09_confirm',
        'TP09','TP10_confirm',
        'TP10',
        'TP11',
        'TP12',
        'ASHA_NPS',
        'SurveySource'
    ]
    
    base_query = f"select {', '.join(target_columns)} from {base_table};"
    base_cursor.execute(base_query)
    
    # Fetch all the results
    base_result = base_cursor.fetchall()
    
    # Convert the result to a dataframe
    df = pd.DataFrame(base_result, columns=base_cursor.column_names)
    
    # Close the connections
    base_cursor.close()
    base_db.close()
    
    # normalise the table
    # prepare for transformation  
    ids = ['IDSK', 'SurveySource', 'ID', 'EmailAddress', 'StartTime']
    values = ['TP01',
            'TP02_TP03_confirm',
            'TP02',
            'TP03',
            'TP04',
            'TP05',
            'TP06',
            'TP07',
            'TP08',
            'TP09_confirm',
            'TP09','TP10_confirm',
            'TP10',
            'TP11',
            'TP12',
            'ASHA_NPS'
    ]
    value_name = 'ResponseText'
    var_name = 'Question'

    # transform the dataframe
    pdf = df.melt(id_vars=ids, 
                value_vars=values,
                value_name=value_name,
                var_name=var_name
                )
    
    # rename columns as required
    pdf = pdf.rename(columns={'Question' : 'QuestionCode'})
    pdf = pdf.sort_values(by=['ID', 'QuestionCode'], ascending=[True, True]).reset_index(drop=True)
    
    # we need to clean the data to replace blank value dues to no response
    pdf.loc[(pdf['QuestionCode'].isin(['TP02', 'TP03', 'TP09', 'TP10'])) & pdf['ResponseText'].isna(), 'ResponseText'] = 'None'
    pdf.loc[(pdf['QuestionCode'].isin(['ASHA_NPS'])) & pdf['ResponseText'].isna(), 'ResponseText'] = '0'
    pdf['ResponseText'] = pdf['ResponseText'].astype(str)
    
    # Establish connection to domain
    domain_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='domain'
        )
    
    domain_cursor = domain_db.cursor()
    
    # join with the TSM scale responses table
    domain_query = f"select Response_SK, Scale_SK, ResponseText from domain.dim__tsm_scale_responses;"
    domain_cursor.execute(domain_query)
    
    # Fetch all the results
    domain_result = domain_cursor.fetchall()
    
    # Convert the result to a dataframe
    domain_df = pd.DataFrame(domain_result, columns=domain_cursor.column_names)
    
    # join onto the dataframe
    pdf = pdf.merge(domain_df, how='left', on='ResponseText')
    
    column_data_types = {
        "IDSK": "VARCHAR(30)",
        "SurveySource" : "VARCHAR(30)",
        "ID" : "INT NOT NULL",
        "EmailAddress" : "VARCHAR(100)",
        "StartTime" : "DATETIME",
        "QuestionCode" : "VARCHAR(50)",
        "ResponseText" : "VARCHAR(50)",
        "Response_SK" : "INT",
        "Scale_SK" : "INT"
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
        for _, row in pdf.iterrows():
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
    base_table = 'tsm_responses'
    domain_table = 'fact__tsm_responses'
    
    create_fact_tsm_responses(host, user, root_pass, base_table, domain_table)