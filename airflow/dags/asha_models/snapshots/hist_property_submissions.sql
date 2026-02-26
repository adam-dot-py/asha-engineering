{% snapshot hist_property_submissions %}
{{
    config(
        target_schema='history',
        unique_key='sr_no',
        strategy='check',
        check_cols='all',
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_property_submissions.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_property_submissions') }}
order by sr_no asc
{% endsnapshot %}
