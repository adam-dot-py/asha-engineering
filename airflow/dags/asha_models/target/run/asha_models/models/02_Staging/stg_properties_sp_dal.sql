
  
  create view "asha_prod"."main_staging"."stg_properties_sp_dal__dbt_tmp" as (
    WITH latest_snapshot AS (
    SELECT *
    FROM "asha_prod"."main_bronze"."raw_properties_sp_dal"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
  );
