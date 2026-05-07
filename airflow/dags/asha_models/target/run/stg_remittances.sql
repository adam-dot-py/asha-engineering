
  
  create view "asha_dev"."main_staging"."stg_remittances__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT
      support_providers,
      cycle,
      coalesce(try_cast(value as double), 0.0) as value,
      ingested_at_ts,
      source_file
    FROM "asha_dev"."main_bronze"."raw_remittances"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY support_providers ORDER BY ingested_at_ts DESC) = 1
)

SELECT
  support_providers,
  cycle,
  value,
  ingested_at_ts,
  source_file
FROM latest_snapshot
  );
