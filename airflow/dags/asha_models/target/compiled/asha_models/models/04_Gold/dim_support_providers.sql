SELECT
    CAST(hash(hp) % 9223372036854775807 AS BIGINT) AS support_provider_id,
    hp AS support_providers
FROM read_json('/home/asha/airflow/support-providers-config.json'),
     UNNEST(housing_providers) AS t(hp)