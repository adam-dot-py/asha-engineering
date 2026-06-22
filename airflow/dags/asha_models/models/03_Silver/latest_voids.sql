{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_voids.parquet' (FORMAT PARQUET)"
)}}

with latest_voids
as (
select
  *
from {{ ref('hist_voids') }}
where dbt_valid_to IS NULL
),

latest_units as (
  select
    *
  from {{ ref('hist_units') }}
  where dbt_valid_to IS NULL
)

select
  v.support_provider_id,
  v.support_providers,
  v.cycle,
  v.value,
  u.units,
  v.ingested_at_ts,
  v.source_file
from latest_voids v
left join latest_units u
  on v.support_provider_id = u.support_provider_id

