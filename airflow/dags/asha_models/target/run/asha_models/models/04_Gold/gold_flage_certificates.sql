
  
    
    

    create  table
      "asha_prod"."main_gold"."gold_flage_certificates__dbt_tmp"
  
    as (
      

SELECT
  a.id as ID,
  a.support_providers as original_support_providers,
  r.support_providers as SupportProviders,
  a.gas_engineer as GasEngineer,
  a.registered_online as Registeredonline,
  a.epc_engineer as EPCEngineer,
  a.registered_online2 as Registeredonline2,
  a.eicr_engineer as EICREngineer,
  a.registered_online3 as Registeredonline3,
  a.napit_engineer as NAPITEngineer,
  a.registered_online4 as Registeredonline4,
  a.ingested_at_ts,
  a.source_file,
  a.dbt_scd_id,
  a.dbt_updated_at,
  a.dbt_valid_from,
  a.dbt_valid_to
from "asha_prod"."main_silver"."latest_flage_certificates" a
left join lateral (
  select
    s.support_providers
  from "asha_prod"."main_reference"."ref_support_providers" s
  order by
    case
      when lower(trim(a.support_providers)) = lower(trim(s.support_providers)) then 0
      else levenshtein(lower(trim(a.support_providers)), lower(trim(s.support_providers)))
    end,
    s.support_providers
  limit 1
) r on true
    );
  
  