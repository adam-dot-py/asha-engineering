{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_clawbacks.parquet' (FORMAT PARQUET)"
)}}

SELECT
  a.support_provider_id as SupportProviderID,
  a.support_providers as original_support_providers,
  r.support_providers as SupportProviders,
  a.cycle as Cycle,
  a.value as Value,
  a.ingested_at_ts,
  a.source_file,
  a.dbt_scd_id,
  a.dbt_updated_at,
  a.dbt_valid_from,
  a.dbt_valid_to
from {{ ref('latest_clawbacks') }} a
left join lateral (
  select
    s.support_providers
  from {{ ref('ref_support_providers') }} s
  order by
    case
      when lower(trim(a.support_providers)) = lower(trim(s.support_providers)) then 0
      else levenshtein(lower(trim(a.support_providers)), lower(trim(s.support_providers)))
    end,
    s.support_providers
  limit 1
) r on true
