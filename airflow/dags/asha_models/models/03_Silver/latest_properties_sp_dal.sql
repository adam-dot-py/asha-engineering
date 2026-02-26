{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_properties_sp_dal.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_properties_sp_dal') }}
where dbt_valid_to is null
order by id asc