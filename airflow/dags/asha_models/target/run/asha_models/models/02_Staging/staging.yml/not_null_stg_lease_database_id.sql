
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select id
from "asha_prod"."main_staging"."stg_lease_database"
where id is null



  
  
      
    ) dbt_internal_test