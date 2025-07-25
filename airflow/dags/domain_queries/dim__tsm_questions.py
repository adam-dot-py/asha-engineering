# packages
import duckdb
import time
import json
import pandas as pd
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

@log_execution
@motherduck_connection(token=token)
def create_domain_tsm_questions_table(schema, domain_table, con, **kwargs) -> None:
    
    # connect with motherduck
    con.sql("USE asha_production;")
    con.sql(f"CREATE SCHEMA IF NOT EXISTS {schema};")
 
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
    
    tsm_domain_areas = {
        "ID" : "User Information",
        "Start time" : 'User Information',
        "Email" : "User Information", 
        "Name" : "User Information",
        "Taking everything into account, how satisfied or dissatisfied are you with the service provided by Ash Shahada?" : "Overall Satisfaction",
        "Has Ash Shahada carried out a repair to your home in the last 12 months?" : "Context Question",
        "How satisfied or dissatisfied are you with the overall repairs service from Ash Shahada over the last 12 months?" : "Keeping properties in good repair",
        "How satisfied or dissatisfied are you with the time taken to complete your most recent repair after you reported it?" : "Keeping properties in good repair",
        "How satisfied or dissatisfied are you that Ash Shahada provides a home that is well maintained?" : "Keeping properties in good repair",
        "Thinking about the condition of the property or building you live in, how satisfied or dissatisfied are you that Ash Shahada provides a home that is safe?" : "Maintaining building safety",
        "How satisfied or dissatisfied are you that Ash Shahada listens to your views and acts upon them?" : "Respectful and helpful engagement",
        "How satisfied or dissatisfied are you that Ash Shahada keeps you informed about things that matter to you?" :  "Respectful and helpful engagement",
        'To what extent do you agree or disagree with the following “Ash Shahada treats me fairly and with respect”?' : "Respectful and helpful engagement",
        "Have you made a complaint to Ash Shahada in the last 12 months?" :  "Context Question",
        "How satisfied or dissatisfied are you with Ash Shahada’s approach to complaints handling?" :  "Effective handling of complaints",
        "Do you live in a building with communal areas, either inside or outside, that Ash Shahada is responsible for maintaining" : "Context Question",
        "How satisfied or dissatisfied are you that Ash Shahada keeps these communal areas clean and well maintained?" :  "Responsible neighbourhood management",
        "How satisfied or dissatisfied are you that Ash Shahada makes a positive contribution to your neighbourhood?" : "Responsible neighbourhood management",
        "How satisfied or dissatisfied are you with Ash Shahada’s approach to handling anti-social behaviour?" : "Responsible neighbourhood management",
        "How likely are you to recommend us to a friend or colleague?" : "Net Promoter Score",
        "To help us improve, please leave a comment to explain any of your scorings." : "Comment",
        "SurveySource" : "Survey Information",
        "IDSK" : "Survey Information"
    }
    
    tsm_measure_names = {
        "TP01" : "Overall satisfaction",
        "TP02" : "Satisfaction with repairs",
        "TP03" : "Satisfaction with time taken to complete most recent repair",
        "TP04" : "Satisfaction that the home is well-maintained",
        "TP05" : "Satisfaction that the home is safe",
        "TP06" : "Satisfaction that the landlord listens to tenant views and acts upon them",
        "TP07" : "Satisfaction that the landlord keeps tenants informed about things that matter to them",
        "TP08" : "Agreement that the landlord treats tenants fairly and with respect",
        "TP09" : "Satisfaction with the landlord’s approach to handling of complaints",
        "TP11" : "Satisfaction that the landlord makes a positive contribution to neighbourhoods",
        "TP12" : "Satisfaction with the landlord’s approach to handling anti-social behaviour"
    }

    df = pd.DataFrame.from_dict(data=tsm_domain_column_names, orient="index").reset_index()
    df['Order'] = range(1, len(df) + 1)
    df.columns = ["QuestionText", "QuestionCode", "Order"]
    
    # add the section names
    df['Section'] = df['QuestionText'].apply(lambda x: tsm_domain_areas.get(x, None))
    df['TSMMeasure'] = df['QuestionCode'].apply(lambda x: tsm_measure_names.get(x, "No measures"))
    
    # establish df order
    df = df[["QuestionText", "QuestionCode", "Section", "TSMMeasure", "Order"]]
    
    # write to motherduck
    con.sql(f"CREATE OR REPLACE TABLE {schema}.{domain_table} AS SELECT * FROM df;")
    con.close()
    
if __name__ == '__main__':
    
    schema = 'silver'
    domain_table = 'dim__tsm_questions'

    create_domain_tsm_questions_table(schema, domain_table)