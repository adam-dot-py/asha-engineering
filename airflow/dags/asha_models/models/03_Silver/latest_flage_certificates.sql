{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_flage_certificates.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_flage_certificates') }}
where dbt_valid_to IS NULL