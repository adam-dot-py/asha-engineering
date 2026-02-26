{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_clawbacks.parquet' (FORMAT PARQUET)"
)}}

select
  *
from {{ ref('hist_clawbacks') }}
where dbt_valid_to IS NULL