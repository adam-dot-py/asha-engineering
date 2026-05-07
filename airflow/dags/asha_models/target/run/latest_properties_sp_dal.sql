
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_properties_sp_dal__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_properties_sp_dal"
where dbt_valid_to is null
order by id asc
    );
  
  