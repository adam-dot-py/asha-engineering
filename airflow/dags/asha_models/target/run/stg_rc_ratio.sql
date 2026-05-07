
  
  create view "asha_dev"."main_staging"."stg_rc_ratio__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT
      *
    FROM "asha_dev"."main_bronze"."raw_rc_ratio"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY support_providers ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
WHERE cycle != 'id'
  );
