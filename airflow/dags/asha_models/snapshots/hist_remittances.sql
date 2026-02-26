{% snapshot hist_remittances %}
{{
    config(
        target_schema='history',
        unique_key='support_providers',
        strategy='check',
        check_cols=[
            'cycle',
            'value'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_remittances.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_remittances') }}
{% endsnapshot %}