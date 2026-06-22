WITH latest_snapshot AS (
    SELECT 
      *
    FROM {{ ref('raw_leavers') }}
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM {{ ref('raw_leavers') }}
    )
)

SELECT *
FROM latest_snapshot