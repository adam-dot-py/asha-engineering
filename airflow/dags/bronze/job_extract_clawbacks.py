# append custom function path and bring in lookup
import sys
sys.path.append('/home/asha/airflow/dags/custom_functions')
from lookup_support_provider import lookup_support_provider

# packages
import os
import json
import time
import polars as pl
from datetime import datetime

def extract_clawbacks(target_source_path: str, target_sheet: str, table_name: str,  **kwargs):
        
    ingested_at_ts = datetime.now()
    file_name = f"snap_{table_name}_{ingested_at_ts.strftime('%Y%m%d%H%M%S')}.parquet"
    for root, dirs, files in os.walk(target_source_path):
        for file in files:
            if file == "PBI DATA - Copy.xlsx":
                filePath = os.path.join(root, file)
                polarsDF = pl.read_excel(
                    filePath,
                    sheet_name=target_sheet
                )
                
                # sterilise column headers
                new_columns = [col.replace(' ', '_').lower() for col in polarsDF.columns]
                polarsDF.columns = new_columns
                
                # remove total line or blanks
                polarsDF = polarsDF.filter(
                    (pl.col('support_providers') != 'Grand Total')
                )
                polarsDF = polarsDF.drop_nulls(subset=['support_providers'])
                
                # this table requires melting - bad practice, but advisable
                idx_cols = ['support_providers']
                on_cols = [c for c in polarsDF.columns if c not in idx_cols]
                meltedDF = polarsDF.unpivot(
                    on=on_cols,
                    index=idx_cols,
                    variable_name='cycle',
                    value_name='value'
                )
                              
                # add snapshot metadata
                meltedDF = meltedDF.with_columns(
                    pl.lit(ingested_at_ts).alias('ingested_at_ts'),
                    pl.lit(file).alias('source_file')
                )
                
                # write to parquet / object storage
                save_dir_name = f"/home/asha/airflow/dags/bronze/raw/raw_{table_name}"
                os.makedirs(save_dir_name, exist_ok=True)
                meltedDF.write_parquet(f'{save_dir_name}/{file_name}')

if __name__ == "__main__":
    
    # import target source config
    target_source_config = "/home/asha/airflow/target-source-config.json"
        
    with open(target_source_config, "r") as t_con:
        target_config = json.load(t_con)
    
    # this is the ETL task
    target_source_path = target_config.get("target_source_path")
    schema = "bronze"
    table_name = 'clawbacks'
    target_sheet = 'Clawbacks'

    extract_clawbacks(
        table_name=table_name,
        target_source_path=target_source_path,
        target_sheet=target_sheet
    )