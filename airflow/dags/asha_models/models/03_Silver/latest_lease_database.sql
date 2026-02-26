{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_lease_database.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_lease_database') }}
where dbt_valid_to is null
order by id asc