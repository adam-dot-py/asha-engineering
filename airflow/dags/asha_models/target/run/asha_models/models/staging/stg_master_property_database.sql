
  
  create view "asha_dev"."main_staging"."stg_master_property_database__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT *
    FROM "asha_dev"."bronze"."raw_master_property_database"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
  );
