{% snapshot hist_lease_terminations %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols=[
            'support_providers',
            'property_address',
            'units',
            'lease_termination_date'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_lease_database.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_lease_terminations') }}
{% endsnapshot %}