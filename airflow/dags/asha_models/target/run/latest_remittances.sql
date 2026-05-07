
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_remittances__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_remittances"
where dbt_valid_to is null
    );
  
  