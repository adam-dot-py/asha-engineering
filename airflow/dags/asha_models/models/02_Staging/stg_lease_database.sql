WITH latest_snapshot AS (
    SELECT 
      CAST(hash(support_providers) % 9223372036854775807 AS BIGINT) AS support_provider_id,
      *
    FROM {{ ref('raw_lease_database') }}
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM {{ ref('raw_lease_database') }}
    )
    and id is not null
)

SELECT *
FROM latest_snapshot