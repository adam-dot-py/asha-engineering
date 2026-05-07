WITH latest_snapshot AS (
    SELECT
      *
    FROM "asha_prod"."main_bronze"."raw_property_submissions"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY sr_no ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot