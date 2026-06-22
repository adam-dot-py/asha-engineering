WITH latest_snapshot AS (
    SELECT *
    FROM "asha_dev"."main_bronze"."raw_leavers"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sr_no ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot