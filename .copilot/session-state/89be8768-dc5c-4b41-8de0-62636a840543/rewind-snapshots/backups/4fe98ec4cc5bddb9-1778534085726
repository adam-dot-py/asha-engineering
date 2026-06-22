WITH latest_snapshot AS (
    SELECT *
    FROM "asha_dev"."main_bronze"."raw_master_property_database"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot