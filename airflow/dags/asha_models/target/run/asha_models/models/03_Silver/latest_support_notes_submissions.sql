
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_support_notes_submissions__dbt_tmp"
  
    as (
      


select
  *
from "asha_prod"."history"."hist_support_notes_submissions"
where dbt_valid_to IS NULL
    );
  
  