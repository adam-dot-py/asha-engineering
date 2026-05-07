
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_master_property_database__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_master_property_database"
where dbt_valid_to IS NULL
    );
  
  