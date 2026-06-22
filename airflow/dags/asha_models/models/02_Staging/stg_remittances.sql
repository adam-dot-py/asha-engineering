WITH latest_snapshot AS (
    SELECT 
      CAST(hash(support_providers) % 9223372036854775807 AS BIGINT) AS support_provider_id,
      *
    FROM {{ ref('raw_remittances') }}
    WHERE ingested_at_ts = (
      SELECT max(ingested_at_ts)
      FROM {{ ref('raw_remittances') }}
    )
)

SELECT
  support_provider_id,
  support_providers,
  upper(cycle) as cycle,
  value,
  ingested_at_ts,
  source_file
FROM latest_snapshot