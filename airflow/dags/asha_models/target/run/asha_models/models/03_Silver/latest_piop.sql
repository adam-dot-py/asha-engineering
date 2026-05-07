
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_piop__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_piop"
where dbt_valid_to is null
order by cycle asc
    );
  
  