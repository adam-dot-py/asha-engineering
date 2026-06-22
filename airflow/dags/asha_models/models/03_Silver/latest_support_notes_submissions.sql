{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_support_notes_submissions.parquet' (FORMAT PARQUET)"
)}}


select
  *
from {{ ref('hist_support_notes_submissions') }}
where dbt_valid_to IS NULL