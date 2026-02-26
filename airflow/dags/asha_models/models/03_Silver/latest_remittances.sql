{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_remittances.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_remittances') }}
where dbt_valid_to is null