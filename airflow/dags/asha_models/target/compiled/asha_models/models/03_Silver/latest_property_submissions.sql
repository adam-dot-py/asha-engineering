


select
  sr_no,
  property_address,
  units,
  support_providers,
  submission_date,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_prod"."history"."hist_property_submissions"
where dbt_valid_to is null
order by sr_no asc