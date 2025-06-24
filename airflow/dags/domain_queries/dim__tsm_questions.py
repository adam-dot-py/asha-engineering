# packages
import json
import pandas as pd
import numpy as np
import mysql.connector

def create_domain_tsm_questions_table(host, user, root_pass, domain_table, **kwargs) -> None:
    
    # Establish connection to domain
    domain_db = mysql.connector.connect(
            host=host,
            user=user,
            password=root_pass,
            database='domain'
        )
    
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
    
    # esablish connection to domain
    domain_cursor = domain_db.cursor()
    
    column_data_types = {
        "QuestionText" : "VARCHAR(200)",
        "QuestionCode" : "VARCHAR(20)",
        "Section" : "VARCHAR(100)",
        "TSMMeasure" : "VARCHAR(100)",
        "`Order`" : "INT NOT NULL"
    }
    
    column_headers = [f"{col} {data_type}" for col, data_type in column_data_types.items()]
    column_headers_string = ", ".join(column_headers)
    print(column_headers_string)
    
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
    domain_table = 'dim__tsm_questions'

    create_domain_tsm_questions_table(host, user, root_pass, domain_table)