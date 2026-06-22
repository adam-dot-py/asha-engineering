
  
    
    

    create  table
      "asha_prod"."main_gold"."gold_property_submissions__dbt_tmp"
  
    as (
      

SELECT
  a.sr_no as SrNo,
  a.property_address as PropertyAddress,
  a.units as Units,
  a.support_providers as original_support_providers,
  r.support_providers as SupportProviders,
  a.submission_date as SubmissionDate,
  a.ingested_at_ts,
  a.source_file,
  a.dbt_scd_id,
  a.dbt_updated_at,
  a.dbt_valid_from,
  a.dbt_valid_to
from "asha_prod"."main_silver"."latest_property_submissions" a
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
  
  