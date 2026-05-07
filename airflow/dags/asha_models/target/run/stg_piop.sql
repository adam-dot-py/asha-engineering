
  
  create view "asha_dev"."main_staging"."stg_piop__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT *
    FROM "asha_dev"."main_bronze"."raw_piop"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cycle ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
  );
