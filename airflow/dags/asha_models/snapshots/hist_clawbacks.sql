{% snapshot hist_clawbacks %}
{{
    config(
        target_schema='history',
        unique_key='support_provider_id',
        strategy='check',
        check_cols=[
            'support_providers',
            'cycle',
            'value'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_clawbacks.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_clawbacks') }}
{% endsnapshot %}