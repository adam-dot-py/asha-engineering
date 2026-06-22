WITH latest_snapshot AS (
    SELECT 
      *
    FROM "asha_dev"."main_bronze"."raw_piop"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_dev"."main_bronze"."raw_piop"
    )
)

SELECT *
FROM latest_snapshot