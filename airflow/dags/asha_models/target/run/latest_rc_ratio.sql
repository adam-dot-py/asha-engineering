
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_rc_ratio__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_rc_ratio"
where dbt_valid_to is null
    );
  
  