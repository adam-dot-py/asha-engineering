
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_rc_ratio__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_rc_ratio"
where dbt_valid_to is null
    );
  
  