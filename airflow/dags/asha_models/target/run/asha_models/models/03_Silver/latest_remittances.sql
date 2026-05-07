
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_remittances__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_remittances"
where dbt_valid_to is null
    );
  
  