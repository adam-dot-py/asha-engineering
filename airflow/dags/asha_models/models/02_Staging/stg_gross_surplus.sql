WITH latest_snapshot AS (
    SELECT 
      *
    FROM {{ ref('raw_gross_surplus') }}
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM {{ ref('raw_gross_surplus') }}
    )
)

SELECT *
FROM latest_snapshot