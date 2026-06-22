
  
  create view "asha_prod"."main_staging"."stg_support_notes_submissions__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT 
      CAST(hash(support_providers) % 9223372036854775807 AS BIGINT) AS support_provider_id,
      *
    FROM "asha_prod"."main_bronze"."raw_support_notes_submissions"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_prod"."main_bronze"."raw_support_notes_submissions"
    )
)

SELECT
  *
FROM latest_snapshot
  );
