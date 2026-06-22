
  
    
    

    create  table
      "asha_prod"."main_silver"."latest_properties_sp_dal__dbt_tmp"
  
    as (
      


select
  id,
  support_providers,
  property_address,
  number_of_units,
  "is_the_landlord_same_as_company_director?_(yes_or_no)" as is_the_landlord_same_as_company_director_yes_or_no,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_prod"."history"."hist_properties_sp_dal"
where dbt_valid_to is null
order by id asc
    );
  
  