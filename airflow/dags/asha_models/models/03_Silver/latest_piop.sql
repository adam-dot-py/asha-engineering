{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_piop.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_piop') }}
where dbt_valid_to is null
order by cycle asc