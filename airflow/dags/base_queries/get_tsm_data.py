# packages
import re
import json
import pandas as pd
import numpy as np
import mysql.connector
from pathlib import Path
from datetime import datetime

# create a function to extract the data
def create_base_tsm_table(host: str, user: str, root_pass: str, db_name: str, table_name: str, tsm_file: str, tsm_sea_file: str) -> None:
    """Create relevant base data, transform and offload to the database and base table.

    Args:
        host (str): the localhost to connect to
        user (str): the username of the database
        root_pass (str): the password of the user of the database
        db_name (str): the name of the database
        table_name (str): the name of the table
        tsm_file (str): the path to the Excel file that is being populated with survey responses
    """
    
    # Read in the TSM raw excel file
    df_tsm = pd.read_excel(tsm_file, sheet_name='responses')
    df_tsm['SurveySource'] = "TSM Survey"
    df_tsm['IDSK'] = df_tsm['ID'].apply(lambda x: str(x) + "-tsm")
    
    # Read in the TSM SEA excel file
    df_tsm_sea = pd.read_excel(tsm_sea_file, sheet_name='responses')
    df_tsm_sea['SurveySource'] = "TSM SEA Survey"
    df_tsm_sea['IDSK'] = df_tsm_sea['ID'].apply(lambda x: str(x) + "-tsm-sea")
    
    # join them both back together
    df = pd.concat([df_tsm, df_tsm_sea]).reset_index(drop=True)
    
    ## TODO: Need to add ID with tsm and tsm_sea

    # clean the data
    # First we define our new column names based on existing columns. This dictionaBry will change the header to a new identifier.
    tsm_domain_column_names = {
        "ID" : "ID",
        "Start time" : 'StartTime',
        "Email" : "EmailAddress", 
        "Name" : "Name",
        "Taking everything into account, how satisfied or dissatisfied are you with the service provided by Ash Shahada?" : "TP01",
        "Has Ash Shahada carried out a repair to your home in the last 12 months?" : "TP02_TP03_confirm",
        "How satisfied or dissatisfied are you with the overall repairs service from Ash Shahada over the last 12 months?" : "TP02",
        "How satisfied or dissatisfied are you with the time taken to complete your most recent repair after you reported it?" : "TP03",
        "How satisfied or dissatisfied are you that Ash Shahada provides a home that is well maintained?" : "TP04",
        "Thinking about the condition of the property or building you live in, how satisfied or dissatisfied are you that Ash Shahada provides a home that is safe?" : "TP05",
        "How satisfied or dissatisfied are you that Ash Shahada listens to your views and acts upon them?" : "TP06",
        "How satisfied or dissatisfied are you that Ash Shahada keeps you informed about things that matter to you?" :  "TP07",
        'To what extent do you agree or disagree with the following “Ash Shahada treats me fairly and with respect”?' : "TP08",
        "Have you made a complaint to Ash Shahada in the last 12 months?" :  "TP09_confirm",
        "How satisfied or dissatisfied are you with Ash Shahada’s approach to complaints handling?" :  "TP09",
        "Do you live in a building with communal areas, either inside or outside, that Ash Shahada is responsible for maintaining" : "TP10_confirm",
        "How satisfied or dissatisfied are you that Ash Shahada keeps these communal areas clean and well maintained?" :  "TP10",
        "How satisfied or dissatisfied are you that Ash Shahada makes a positive contribution to your neighbourhood?" : "TP11",
        "How satisfied or dissatisfied are you with Ash Shahada’s approach to handling anti-social behaviour?" : "TP12",
        "How likely are you to recommend us to a friend or colleague?" : "ASHA_NPS",
        "To help us improve, please leave a comment to explain any of your scorings." : "ASHA_COMMENT",
        "SurveySource" : "SurveySource",
        "IDSK" : "IDSK"
    }
    
    # This dictionary defines the datatypes of the new column headers and will be used in the SQL query
    column_data_types = {
    'ID': 'INT NOT NULL',
    'StartTime': 'DATETIME',
    'EmailAddress': 'VARCHAR(255)',
    'Name': 'VARCHAR(255)',
    'TP01': 'VARCHAR(255)',
    'TP02_TP03_confirm': 'VARCHAR(255)',
    'TP02': 'VARCHAR(255)',
    'TP03': 'VARCHAR(255)',
    'TP04': 'VARCHAR(255)',
    'TP05': 'VARCHAR(255)',
    'TP06': 'VARCHAR(255)',
    'TP07': 'VARCHAR(255)',
    'TP08': 'VARCHAR(255)',
    'TP09_confirm': 'VARCHAR(255)',
    'TP09': 'VARCHAR(255)',
    'TP10_confirm': 'VARCHAR(255)',
    'TP10': 'VARCHAR(255)',
    'TP11': 'VARCHAR(255)',
    'TP12': 'VARCHAR(255)',
    'ASHA_NPS': 'INT',
    'ASHA_COMMENT' : 'LONGTEXT',
    'SurveySource' : 'VARCHAR(30)',
    'IDSK': 'VARCHAR(30)',
    }
    
    # generate new column headers and assign to the raw dataframe
    tsm_column_headers = []
    for k, v in tsm_domain_column_names.items():
        tsm_column_headers.append(v)
    
    # set the new headers
    df.columns = tsm_column_headers
    
    # Execute the query
    try:
        # Establish connection to the server
        mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database=db_name
        )

        # create mysql connection
        print(f"Attempting connection to {db_name}")
        
        # Create a cursor object
        mycursor = mydb.cursor()
        
        # this will create a string which is passed into the SQL query. We take the column header and its respective data type as defined above.
        column_headers = [f"`{col}` {data_type}" for col, data_type in column_data_types.items()]
        # column_headers.append('PRIMARY KEY (ID)')
        column_headers_string = ', '.join(column_headers)
        
        # drop the table first
        try:
            drop_query = f"drop table {table_name};"
            mycursor.execute(drop_query)
            print(f"Dropped -> {table_name}")
        except Exception as e:
          pass
        
        # create the table again
        create_table_query = "CREATE TABLE IF NOT EXISTS {table_name} ({column_headers_string})".format(table_name = table_name, column_headers_string = column_headers_string)
        print(f"Create query -> {create_table_query}")
        mycursor.execute(create_table_query)
        print(f"Recreated -> {table_name}")
        print("Connection successful")

        # add data to our database
        for _, row in df.iterrows():
            
            # generate insert statement
            values = []
            for value in row:
                if isinstance(value, int):
                    values.append(f"'{value}'")
                elif pd.isnull(value):
                    values.append('NULL')
                elif isinstance(value, str):
                    value = value.strip()  # Remove leading and trailing whitespace
                    if value.lower() == "not applicable/ don’t know":
                        value = "not applicable/ do not know"
                    if value.lower() == "don’t know":
                        value = "do not know"
                    # account for mistakes in question labels
                    if value.lower() == "fairly satisifed":
                        value = "fairly satisfied"
                    if value.lower() == "fairly disatisifed":
                        value = "fairly dissatisfied"
                    else: value = re.sub(r"'", r"/", value).strip().lower() # replace apostrophre's with forward slash, strip and lower
                    values.append(f"'{value}'")
                elif isinstance(value, pd.Timestamp):
                    values.append(f"'{value}'")
                    
            # create the values string, to be passed in the query
            values_string = ', '.join(values)
            insert_query = f"insert into {table_name} values ({values_string})"
            
            # execute and commit changes
            mycursor.execute(insert_query)
            mydb.commit()
        print(f"Successfully created / updated -> {table_name}")
    except Exception as e:
        print(f"Error -> {e}")
    finally:
        # Close the cursor and connection to the database
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()

if __name__ == "__main__":
    
    # import server config file
    server_config = "/home/asha/airflow/server-config.json"

    with open(server_config, "r") as fp:
        config = json.load(fp)

    # prepare the details to connect to the databases
    host = config.get("host")
    user = config.get("user")
    root_pass = config.get("root_pass")
    
    # this is the path to the tsm-responses file
    tsm_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-responses.xlsx")
    tsm_sea_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-sea-responses.xlsx")

    # prepare the details to connect to the database
    db_name = "base"
    table_name = 'tsm_responses'
    
    # execute the function
    create_base_tsm_table(host, user, root_pass, db_name, table_name, tsm_file, tsm_sea_file)