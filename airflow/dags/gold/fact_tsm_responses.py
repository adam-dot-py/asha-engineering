# packages
import duckdb
import pandas as pd
import polars as pld
import numpy as np
import os
import json

def create_fact_tsm_responses(silver_table, gold_table):
    """__summary__

    Args:
    __args__
    
    """
    
    con = duckdb.connect('/home/asha/airflow/database/asha_prod.duckdb')
    df = con.sql(f'select * from main_silver.{silver_table};').df()
    
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
    
    ref_df = con.sql('select * from main_reference.ref_tsm_scales;').df()
    pdf = pdf.merge(ref_df, how='left', on='ResponseText')
    
    con.sql(f'create or replace table main_gold.{gold_table} as ( select * from pdf );')
    con.sql(f"COPY main_gold.{gold_table} TO '/home/asha/airflow/dags/gold/facts/{gold_table}.parquet' (FORMAT PARQUET);")
    con.close()
    
if __name__ == '__main__':
    
    # # import server config file
    # server_config = "/home/asha/airflow/server-config.json"

    # with open(server_config, "r") as fp:
    #     config = json.load(fp)

    # prepare the details to connect to the databases
    silver_table = 'std_all_tsm_survey_responses'
    gold_table = 'fact_tsm_responses'
    
    create_fact_tsm_responses(silver_table, gold_table)