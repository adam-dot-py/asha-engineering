
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_property_submissions__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_property_submissions"
where dbt_valid_to is null
order by sr_no asc
    );
  
  