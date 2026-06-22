{% snapshot hist_hqi %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols='all',
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_hqi.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_hqi') }}
order by id asc
{% endsnapshot %}
