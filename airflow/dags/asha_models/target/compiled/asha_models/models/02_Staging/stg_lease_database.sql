WITH latest_snapshot AS (
    SELECT *
    FROM "asha_prod"."main_bronze"."raw_lease_database"
    where id is not null
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot