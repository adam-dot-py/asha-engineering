{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_rc_ratio.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_rc_ratio') }}
where dbt_valid_to is null