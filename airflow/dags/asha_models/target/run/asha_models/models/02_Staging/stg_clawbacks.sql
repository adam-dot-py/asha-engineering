
  
  create view "asha_prod"."main_staging"."stg_clawbacks__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT
      CAST(hash(support_providers) % 9223372036854775807 AS BIGINT) AS support_provider_id,
      support_providers,
      cycle,
      value,
      ingested_at_ts,
      source_file
    FROM "asha_prod"."main_bronze"."raw_clawbacks"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY support_providers ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
  );
