
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_clawbacks__dbt_tmp"
  
    as (
      select
  *
from "asha_dev"."history"."hist_clawbacks"
where dbt_valid_to IS NOT NULL
    );
  
  