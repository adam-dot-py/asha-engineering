{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_rc_ratio.parquet' (FORMAT PARQUET)"
)}}


select
  support_providers,
  cycle,
  value,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to,
  support_provider_id
from {{ ref('hist_rc_ratio') }}
where dbt_valid_to is null