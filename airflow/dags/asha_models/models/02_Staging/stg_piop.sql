WITH latest_snapshot AS (
    SELECT 
      *
    FROM {{ ref('raw_piop') }}
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM {{ ref('raw_piop') }}
    )
)

SELECT *
FROM latest_snapshot
