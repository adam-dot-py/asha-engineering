{% snapshot hist_leavers %}
{{
    config(
        target_schema='history',
        unique_key='sr_no',
        strategy='check',
        check_cols='all',
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_leavers.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_leavers') }}
{% endsnapshot %}
