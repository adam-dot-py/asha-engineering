{% snapshot hist_support_notes_submissions %}
{{
    config(
        target_schema='history',
        unique_key='support_providers',
        strategy='check',
        check_cols=[
            'successful_submission',
            'success_pct'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_support_notes_submissions.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_support_notes_submissions') }}
{% endsnapshot %}