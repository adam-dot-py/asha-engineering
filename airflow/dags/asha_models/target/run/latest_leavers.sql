
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_leavers__dbt_tmp"
  
    as (
      

select
  sr_no as SrNo,
  upper(tenant_name) as TenantName,
  concat('****', cast(right(crn, 4) as string))as CRN,  
  upper(leaving_address) as LeavingAddress,
  concat(cast(substring(ninumber, 1, 2) as string), '******') as NINumber,
  cast(dob as date) as DOB,
  cast(vacate_date as date) as VacateDate,
  cast(date_informed as date) as DateInformed,
  notes as Notes,
  ingested_at_ts,
  source_file,
  dbt_scd_id,
  dbt_updated_at,
  dbt_valid_from,
  dbt_valid_to
from "asha_dev"."history"."hist_leavers"
where dbt_valid_to IS NULL
    );
  
  