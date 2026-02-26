WITH latest_snapshot AS (
    SELECT *
    FROM {{ ref('raw_flage_certificates') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot