{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_voids.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_voids') }}
where dbt_valid_to IS NULL