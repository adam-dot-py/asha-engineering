
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_properties_sp_dal__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_properties_sp_dal"
where dbt_valid_to is null
order by id asc
    );
  
  