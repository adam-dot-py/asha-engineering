{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_flage_certificates.parquet' (FORMAT PARQUET)"
)}}


select
  id,
  support_providers,
  gas_engineer,
  registered_online,
  epc_engineer,
  registered_online2,
  eicr_engineer,
  registered_online3,
  napit_engineer,
  registered_online4,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from {{ ref('hist_flage_certificates') }}
where dbt_valid_to IS NULL