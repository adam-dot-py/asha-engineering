
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_voids__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_voids"
where dbt_valid_to IS NULL
    );
  
  