
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_flage_certificates__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_flage_certificates"
where dbt_valid_to IS NULL
    );
  
  