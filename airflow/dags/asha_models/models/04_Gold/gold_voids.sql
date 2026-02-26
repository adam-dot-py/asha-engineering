{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_voids.parquet' (FORMAT PARQUET)"
)}}

SELECT
    a.support_providers as original_support_providers,
    r.support_providers as adj_support_providers,
    a.support_provider_id,
    a.cycle,
    a.value,
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END AS distance
FROM {{ ref('latest_voids') }} a
CROSS JOIN {{ ref('ref_support_providers') }} r
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.support_providers 
    ORDER BY 
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END
) = 1