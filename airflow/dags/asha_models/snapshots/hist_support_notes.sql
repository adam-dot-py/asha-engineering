{% snapshot hist_support_notes %}
{{
    config(
        target_schema='history',
        unique_key=['support_providers', 'cycle'],
        strategy='check',
        check_cols=[
            'value'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_support_notes.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_support_notes') }}
{% endsnapshot %}