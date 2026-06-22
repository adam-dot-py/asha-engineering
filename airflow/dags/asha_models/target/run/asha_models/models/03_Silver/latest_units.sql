
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_units__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_units"
where dbt_valid_to IS NULL
    );
  
  