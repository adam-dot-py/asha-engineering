
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_lease_terminations__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_lease_terminations"
where dbt_valid_to IS NULL
    );
  
  