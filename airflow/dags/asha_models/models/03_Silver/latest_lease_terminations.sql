{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_lease_terminations.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_lease_terminations') }}
where dbt_valid_to IS NULL