import duckdb

con = duckdb.connect('/home/asha/airflow/database/asha_prod.duckdb')

query = """
COPY main_reference.ref_tsm_scales TO '/home/asha/airflow/dags/gold/dimensions/ref_tsm_scales.parquet' (FORMAT PARQUET);
"""

con.sql(query)

con.close()