
  
    
    

    create  table
      "asha_dev"."main_silver"."std.all_tsm_survey_responses__dbt_tmp"
  
    as (
      select
  *
from "asha_dev"."main_silver"."std_tsm_responses"

union all 

select
  *
from "asha_dev"."main_silver"."std_tsm_sea_responses"
    );
  
  