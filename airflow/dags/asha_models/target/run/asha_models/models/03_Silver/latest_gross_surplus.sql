
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_gross_surplus__dbt_tmp"
  
    as (
      


select
  cycle,
  surplus,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_prod"."history"."hist_gross_surplus"
where dbt_valid_to IS NULL
    );
  
  