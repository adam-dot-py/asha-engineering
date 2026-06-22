
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_clawbacks__dbt_tmp"
  
    as (
      

select
  support_provider_id,
  support_providers,
  cycle,
  value,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_prod"."history"."hist_clawbacks"
where dbt_valid_to IS NULL
    );
  
  