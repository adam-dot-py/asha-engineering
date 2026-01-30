{% snapshot hist_flage_certificates %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols=[
            'support_providers',
            'gas_engineer',
            'registered_online',
            'epc_engineer',
            'registered_online2',
            'eicr_engineer',
            'registered_online3',
            'napit_engineer',
            'registered_online4'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_flage_certificates.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_flage_certificates') }}
{% endsnapshot %}