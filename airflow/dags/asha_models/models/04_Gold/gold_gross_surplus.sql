{{config(
	post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_gross_surplus.parquet' (FORMAT PARQUET)"
)}}

select
  cycle as Cycle,
  surplus as Surplus,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from {{ ref('latest_gross_surplus') }}