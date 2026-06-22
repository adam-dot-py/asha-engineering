WITH latest_snapshot AS (
    SELECT 
      *
    FROM "asha_prod"."main_bronze"."raw_gross_surplus"
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM "asha_prod"."main_bronze"."raw_gross_surplus"
    )
)

SELECT *
FROM latest_snapshot