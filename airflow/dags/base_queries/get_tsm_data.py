# packages
import duckdb
import json
import time
import pandas as pd
from pathlib import Path
from functools import wraps

# import motherduck token and target source config
target_source_config = "/home/asha/airflow/target-source-config.json"
server_config = "/home/asha/airflow/duckdb-config.json"
    
with open(target_source_config, "r") as t_con:
    target_config = json.load(t_con)

with open(server_config, "r") as fp:
    config = json.load(fp)
token = config['token']

def log_execution(func):
    """
    """
    
    @wraps(func)
    def etl_task_time(*args, **kwargs):
        start_time = time.time()
        print(f"Starting '{func.__name__}'...")
        result = func(*args, **kwargs)
        print(f"Finished '{func.__name__}' in {time.time() - start_time} seconds.")
        return result

    return etl_task_time

def motherduck_connection(token):
    def connection_decorator(func):
        con = duckdb.connect(f'md:?motherduck_token={token}')
        
        @wraps(func)
        def wrapper(*args, **kwargs):
            # pass con as a keyword argument for use in other functions
            return func(*args, con=con, **kwargs)
    
        return wrapper
    return connection_decorator

# create a function to extract the data
@log_execution
@motherduck_connection(token=token)
def create_base_tsm_table(token, schema, table_name: str, tsm_file: str, tsm_sea_file: str, con, **kwargs) -> None:
    """Create relevant base data, transform and offload to the database and base table.

    Args:
        host (str): the localhost to connect to
        user (str): the username of the database
        root_pass (str): the password of the user of the database
        db_name (str): the name of the database
        table_name (str): the name of the table
        tsm_file (str): the path to the Excel file that is being populated with survey responses
    """
    
    # motherduck
    con.sql("USE asha_production;")
    
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
    
    # write to motherduck
    con.sql(f"CREATE OR REPLACE TABLE {schema}.{table_name} AS SELECT * FROM df;")
    con.close()
    
if __name__ == "__main__":
    
    # this is the path to the tsm-responses file
    tsm_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-responses.xlsx")
    tsm_sea_file = Path(r"/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-sea-responses.xlsx")

    # prepare the details to connect to the database
    schema = "bronze"
    table_name = 'tsm_responses'
    
    # execute the function
    create_base_tsm_table(
        token=token,
        schema=schema,
        table_name=table_name,
        tsm_file=tsm_file,
        tsm_sea_file=tsm_sea_file
    )