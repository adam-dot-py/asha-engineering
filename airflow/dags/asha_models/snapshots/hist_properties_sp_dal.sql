{% snapshot hist_properties_sp_dal %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols='all',
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_properties_sp_dal.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_properties_sp_dal') }}
order by id asc
{% endsnapshot %}
