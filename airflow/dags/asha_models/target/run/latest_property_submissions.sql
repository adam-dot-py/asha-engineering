
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_property_submissions__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_property_submissions"
where dbt_valid_to is null
order by sr_no asc
    );
  
  