


select
  id,
  support_providers,
  total_units_per_provider,
  properties_with_director_as_landlord,
  units_owned_by_support_providers,
  leased_units_with_ash_shahada,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_dev"."history"."hist_dal_summary"
where dbt_valid_to IS NULL