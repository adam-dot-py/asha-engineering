{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_master_property_database.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_master_property_database') }}
where dbt_valid_to IS NULL