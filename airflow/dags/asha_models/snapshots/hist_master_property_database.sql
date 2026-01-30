{% snapshot hist_master_property_database %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols=[
            'property_address',
            'rooms',
            'property_count',
            'support_providers',
            'council_tax_band',
            'property_usage',
            'other_specify'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_master_property_database.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_master_property_database') }}
{% endsnapshot %}
