
  
    
    

    create  table
      "asha_prod"."main_gold"."gold_gross_surplus__dbt_tmp"
  
    as (
      

select
  cycle as Cycle,
  surplus as Surplus,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_prod"."main_silver"."latest_gross_surplus"
    );
  
  