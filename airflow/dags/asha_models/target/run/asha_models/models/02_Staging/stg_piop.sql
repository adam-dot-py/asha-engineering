
  
  create view "asha_prod"."main_staging"."stg_piop__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT 
      *
    FROM "asha_prod"."main_bronze"."raw_piop"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_prod"."main_bronze"."raw_piop"
    )
)

SELECT *
FROM latest_snapshot
  );
