{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/facts/fact_tsm_comments.parquet' (FORMAT PARQUET)"
)}}

select
 IDSK,
 ID, 
 ASHA_COMMENT
from {{ ref('std_all_tsm_survey_responses') }}
where ASHA_COMMENT is not null