{% snapshot hist_units %}
{{
    config(
        target_schema='history',
        unique_key='support_providers',
        strategy='check',
        check_cols=[
            'units'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_units.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_units') }}
{% endsnapshot %}