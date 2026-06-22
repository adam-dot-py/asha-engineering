{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_gross_surplus.parquet' (FORMAT PARQUET)"
)}}


select
  cycle,
  surplus,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from {{ ref('hist_gross_surplus') }}
where dbt_valid_to IS NULL