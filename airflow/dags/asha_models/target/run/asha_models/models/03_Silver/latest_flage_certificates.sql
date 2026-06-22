
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_flage_certificates__dbt_tmp"
  
    as (
      


select
  id,
  support_providers,
  gas_engineer,
  registered_online,
  epc_engineer,
  registered_online2,
  eicr_engineer,
  registered_online3,
  napit_engineer,
  registered_online4,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_prod"."history"."hist_flage_certificates"
where dbt_valid_to IS NULL
    );
  
  