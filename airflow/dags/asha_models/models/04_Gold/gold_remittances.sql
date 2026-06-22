{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_remittances.parquet' (FORMAT PARQUET)"
)}}

SELECT
    a.support_providers as original_support_providers,
    r.support_providers as adj_support_providers,
    a.cycle,
    a.value,
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END AS distance
FROM {{ ref('latest_remittances') }} a
CROSS JOIN {{ ref('ref_support_providers') }} r
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.support_providers, a.cycle
    ORDER BY 
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END
) = 1