WITH latest_snapshot AS (
    SELECT 
      *
    FROM "asha_dev"."main_bronze"."raw_leavers"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_dev"."main_bronze"."raw_leavers"
    )
)

SELECT *
FROM latest_snapshot