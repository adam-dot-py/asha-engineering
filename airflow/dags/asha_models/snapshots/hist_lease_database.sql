{% snapshot hist_lease_database %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols=[
            'lease_start_date',
            'lease_end_date'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_lease_database.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_lease_database') }}
{% endsnapshot %}