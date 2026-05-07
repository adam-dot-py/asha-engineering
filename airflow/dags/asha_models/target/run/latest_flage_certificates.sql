
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_flage_certificates__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_flage_certificates"
where dbt_valid_to IS NULL
    );
  
  