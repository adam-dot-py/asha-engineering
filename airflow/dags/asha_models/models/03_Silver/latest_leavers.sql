{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_leavers.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_leavers') }}
where dbt_valid_to IS NULL