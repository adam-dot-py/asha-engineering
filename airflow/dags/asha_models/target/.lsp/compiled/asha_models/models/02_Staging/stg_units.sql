WITH latest_snapshot AS (
    SELECT 
      CAST(hash(support_providers) % 9223372036854775807 AS BIGINT) AS support_provider_id,
      *
    FROM "asha_dev"."main_bronze"."raw_units"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_dev"."main_bronze"."raw_units"
    )
)

SELECT
  support_provider_id,
  support_providers,
  units,
  ingested_at_ts,
  source_file
FROM latest_snapshot