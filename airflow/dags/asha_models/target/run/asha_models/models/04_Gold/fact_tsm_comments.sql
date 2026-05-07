
  
    
    

    create  table
      "asha_prod"."main_gold"."fact_tsm_comments__dbt_tmp"
  
    as (
      

select
 IDSK,
 ID, 
 ASHA_COMMENT
from "asha_prod"."main_silver"."std_all_tsm_survey_responses"
where ASHA_COMMENT is not null
    );
  
  