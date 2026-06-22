
  
  create view "asha_prod"."main_staging"."stg_leavers__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT 
      *
    FROM "asha_prod"."main_bronze"."raw_leavers"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_prod"."main_bronze"."raw_leavers"
    )
)

SELECT *
FROM latest_snapshot
  );
