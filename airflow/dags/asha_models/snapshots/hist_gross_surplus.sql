{% snapshot hist_gross_surplus %}
{{
    config(
        target_schema='history',
        unique_key='cycle',
        strategy='check',
        check_cols=["surplus"],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_gross_surplus.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_gross_surplus') }}
{% endsnapshot %}
