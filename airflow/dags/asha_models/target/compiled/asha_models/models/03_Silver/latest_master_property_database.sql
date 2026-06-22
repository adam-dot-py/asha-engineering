


select
  id,
  property_address,
  rooms,
  property_count,
  support_providers as provider,
  council_tax_band,
  property_usage,
  other_specify,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to,
  support_providers,
  support_provider_id
  from "asha_prod"."history"."hist_master_property_database"
where dbt_valid_to IS NULL