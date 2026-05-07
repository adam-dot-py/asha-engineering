
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_clawbacks__dbt_tmp"
  
    as (
      

select
  *
from "asha_prod"."history"."hist_clawbacks"
where dbt_valid_to IS NULL
    );
  
  