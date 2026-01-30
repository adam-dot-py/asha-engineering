WITH latest_snapshot AS (
    SELECT *
    FROM {{ source('main_bronze', 'raw_flage_certificates') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot