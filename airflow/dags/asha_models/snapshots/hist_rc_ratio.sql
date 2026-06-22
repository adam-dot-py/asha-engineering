{% snapshot hist_rc_ratio %}
{{
    config(
        target_schema='history',
        unique_key=['support_providers', 'cycle'],
        strategy='check',
        check_cols=[
            'value'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_rc_ratio.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_rc_ratio') }}
{% endsnapshot %}
