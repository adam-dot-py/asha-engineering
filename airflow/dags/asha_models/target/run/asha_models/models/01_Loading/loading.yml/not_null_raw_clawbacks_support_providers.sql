
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select support_providers
from "asha_prod"."main_bronze"."raw_clawbacks"
where support_providers is null



  
  
      
    ) dbt_internal_test