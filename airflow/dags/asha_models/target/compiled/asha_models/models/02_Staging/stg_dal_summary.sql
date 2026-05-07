WITH latest_snapshot AS (
    SELECT *
    FROM "asha_prod"."main_bronze"."raw_dal_summary"
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot