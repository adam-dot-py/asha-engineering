
  
    
    

    create  table
      "asha_dev"."main_gold"."gold_dal_summary__dbt_tmp"
  
    as (
      

SELECT
    a.id,
    a.support_providers as original_support_providers,
    r.support_providers as adj_support_providers,
    a.total_units_per_provider,
    a.properties_with_director_as_landlord,
    a.units_owned_by_support_providers,
    a.leased_units_with_ash_shahada,
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END AS distance
FROM "asha_dev"."main_silver"."latest_dal_summary" a
CROSS JOIN "asha_dev"."main_reference"."ref_support_providers" r
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY a.id 
    ORDER BY 
    CASE 
        WHEN LOWER(TRIM(a.support_providers)) = LOWER(TRIM(r.support_providers)) 
        THEN 0 
        ELSE levenshtein(LOWER(TRIM(a.support_providers)), LOWER(TRIM(r.support_providers)))
    END
) = 1
    );
  
  