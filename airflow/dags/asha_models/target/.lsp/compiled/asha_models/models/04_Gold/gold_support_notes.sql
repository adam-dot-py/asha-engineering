

SELECT
    a.support_provider_id,
    a.id,
  a.support_providers as original_support_providers,
    r.support_providers as SupportProviders,
    a.cycle as Cycle,
    a.value as Value,
  a.successful_submission as SuccessfulSubmission,
  a.success_pct as SuccessPct,
    a.ingested_at_ts,
    a.source_file,
    a.dbt_scd_id,
    a.dbt_updated_at,
    a.dbt_valid_from,
    a.dbt_valid_to
from "asha_dev"."main_silver"."latest_support_notes" a
left join lateral (
  select
    s.support_providers
  from "asha_dev"."main_reference"."ref_support_providers" s
  order by
    case
      when lower(trim(a.support_providers)) = lower(trim(s.support_providers)) then 0
      else levenshtein(lower(trim(a.support_providers)), lower(trim(s.support_providers)))
    end,
    s.support_providers
  limit 1
) r on true