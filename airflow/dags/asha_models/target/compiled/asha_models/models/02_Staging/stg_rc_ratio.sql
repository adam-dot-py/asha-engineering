WITH latest_snapshot AS (
    SELECT
      *
    FROM "asha_prod"."main_bronze"."raw_rc_ratio"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY support_providers ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
WHERE cycle != 'id'