{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_units.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_units') }}
where dbt_valid_to IS NULL