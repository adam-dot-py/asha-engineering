import duckdb

QUERY = """
-- refs
COPY main_reference.ref_tsm_scales TO '/home/asha/airflow/dags/gold/dimensions/ref_tsm_scales.parquet' (FORMAT PARQUET);
COPY main_reference.ref_tsm_scale_types TO '/home/asha/airflow/dags/gold/dimensions/ref_tsm_scale_types.parquet' (FORMAT PARQUET);
"""

def extract_parquets() -> None:
	# Open read-only to reduce lock contention with active writer sessions.
	con = duckdb.connect('/home/asha/airflow/database/asha_prod.duckdb', read_only=True)
	try:
		con.sql(QUERY)
	finally:
		con.close()


if __name__ == '__main__':
	extract_parquets()