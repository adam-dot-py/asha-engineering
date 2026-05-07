
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_lease_database__dbt_tmp"
  
    as (
      


select
  *
from "asha_dev"."history"."hist_lease_database"
where dbt_valid_to is null
order by id asc
    );
  
  