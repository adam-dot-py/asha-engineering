
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_voids__dbt_tmp"
  
    as (
      select
  *
from "asha_dev"."history"."hist_voids"
where dbt_valid_to IS NOT NULL
    );
  
  