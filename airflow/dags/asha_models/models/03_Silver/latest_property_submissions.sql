{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_property_submissions.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_property_submissions') }}
where dbt_valid_to is null
order by sr_no asc