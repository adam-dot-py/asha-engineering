WITH latest_snapshot AS (
    SELECT *
    FROM {{ ref('raw_piop') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cycle ORDER BY ingested_at_ts DESC) = 1
)

SELECT *
FROM latest_snapshot
