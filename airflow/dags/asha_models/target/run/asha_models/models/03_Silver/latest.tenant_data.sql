
  
    
    

    create  table
      "asha_prod"."main_silver"."latest.tenant_data__dbt_tmp"
  
    as (
      WITH ranked AS (
    SELECT *,
        hash(Tenant_SK, PropertyAddress, Room, FirstName, LastName, CycleNumberValue) AS row_hash,
        LAG(hash(Tenant_SK, PropertyAddress, Room, FirstName, LastName, CycleNumberValue)) 
            OVER (PARTITION BY Tenant_SK ORDER BY CycleNumberValue desc) AS prev_row_hash
    FROM "asha_prod"."main_staging"."stg_tenant_data"
)

SELECT 
  *,
   CASE
	   WHEN prev_row_hash IS NULL THEN 'New Record'
       WHEN row_hash != prev_row_hash THEN 'Changed'
       ELSE 'Unchanged'
    END AS record_status
FROM ranked
WHERE CycleNumberValue = (SELECT max(CycleNumberValue) FROM "asha_prod"."main_staging"."stg_tenant_data")
    );
  
  