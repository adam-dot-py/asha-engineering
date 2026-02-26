{% snapshot hist_piop %}
{{
    config(
        target_schema='history',
        unique_key='cycle',
        strategy='check',
        check_cols=[
            'paid_remittances',
            'received_remittances'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_piop.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_piop') }}
order by cycle asc
{% endsnapshot %}
