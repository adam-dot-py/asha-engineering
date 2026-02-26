{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_dal_summary.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_dal_summary') }}
where dbt_valid_to IS NULL