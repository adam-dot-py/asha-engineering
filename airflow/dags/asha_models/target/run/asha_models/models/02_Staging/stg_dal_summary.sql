
  
  create view "asha_prod"."main_staging"."stg_dal_summary__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT 
      CAST(hash(support_providers) % 9223372036854775807 AS BIGINT) AS support_provider_id,
      *
    FROM "asha_prod"."main_bronze"."raw_dal_summary"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_prod"."main_bronze"."raw_dal_summary"
    )
)

SELECT *
FROM latest_snapshot
  );
