


select
  support_providers,
  cycle,
  value,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to,
  support_provider_id
from "asha_dev"."history"."hist_rc_ratio"
where dbt_valid_to is null