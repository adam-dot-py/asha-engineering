{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_master_property_database.parquet' (FORMAT PARQUET)"
)}}

SELECT
  a.id as ID,
  a.property_address as PropertyAddress,
  a.rooms as Rooms,
  a.property_count as PropertyCount,
  a.provider as Provider,
  a.council_tax_band as CouncilTaxBand,
  a.property_usage as PropertyUsage,
  a.other_specify as OtherSpecify,
  a.support_provider_id,
  a.support_providers as original_support_providers,
  r.support_providers as SupportProviders,
  a.ingested_at_ts,
  a.source_file,
  a.dbt_scd_id,
  a.dbt_updated_at,
  a.dbt_valid_from,
  a.dbt_valid_to
from {{ ref('latest_master_property_database') }} a
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

