{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_tenant_data.parquet' (FORMAT PARQUET)"
)}}

with latest as (
select
  *,
  max(CycleNumberValue) as LatestCycleNumberValue
from {{ ref('hist_tenant_data') }}
group by all
)

select 
    Tenant_SK,
    upper(PropertyAddress) as PropertyAddress,
    Room,
    upper(FirstName) as FirstName,
    upper(MiddleName) as MiddleName,
    upper(LastName) as LastName,
    DateOfBirth,
    NINumber,
    -- try_strptime(CheckinDate, ['%d/%m/%y', '%y-%m-%d %H:%M:%S']) as CheckinDate,
    CheckinDate,
    CheckoutDate,
    NewHBClaim,
    HBClaimRefNumber,
    ReferralAgency,
    GroupedReferralAgency,
    Age,
    Gender,
    Religion,
    Ethnicity,
    Nationality,
    Disability,
    SexualOrientation,
    SpokenLanguage,
    RiskAssessment,
    LengthOfStay,
    CycleNumber,
    CycleNumberValue,
    Source,
    ExtractedProviderName,
    ProviderName,
    record_status
from latest