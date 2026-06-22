

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
FROM "asha_dev"."main_silver"."latest_remittances" a
CROSS JOIN "asha_dev"."main_reference"."ref_support_providers" r
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.support_providers, a.cycle
    ORDER BY 
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END
) = 1