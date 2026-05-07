
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_dal_summary__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_dal_summary"
where dbt_valid_to IS NULL
    );
  
  