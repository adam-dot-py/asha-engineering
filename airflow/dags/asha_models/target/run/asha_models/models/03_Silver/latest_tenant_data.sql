
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_tenant_data__dbt_tmp"
  
    as (
      


select *
from "asha_prod"."main_silver"."hist_tenant_data"
qualify CycleNumberValue = max(CycleNumberValue) over (partition by Tenant_SK)
    );
  
  