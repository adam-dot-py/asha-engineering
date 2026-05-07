
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_dal_summary__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_dal_summary"
where dbt_valid_to IS NULL
    );
  
  