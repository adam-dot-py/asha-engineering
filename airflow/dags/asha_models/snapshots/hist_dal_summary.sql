{% snapshot hist_dal_summary %}
{{
    config(
        target_schema='history',
        unique_key='id',
        strategy='check',
        check_cols=[
            'support_providers',
            'total_units_per_provider',
            'properties_with_director_as_landlord',
            'units_owned_by_support_providers',
            'leased_units_with_ash_shahada'
        ],
        post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_dal_summary.parquet' (FORMAT PARQUET)"
    )
}}
select *
from {{ ref('stg_dal_summary') }}
{% endsnapshot %}