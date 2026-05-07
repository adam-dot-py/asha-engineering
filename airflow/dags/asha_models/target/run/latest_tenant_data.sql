
  
    
    

    create  table
      "asha_dev"."main_silver"."latest_tenant_data__dbt_tmp"
  
    as (
      

-- fuzzy lookup cte here first?

with latest as (
select
  *,
  max(CycleNumberValue) as LatestCycleNumberValue
from "asha_dev"."main_silver"."hist_tenant_data"
group by all
),

lev_table as (
  select
    *,
    gen.value as lev_gender,
    levenshtein(lower(Gender), lower(gen.value)) as lev_gender_distance
  from latest
  cross join "asha_dev"."main_reference"."ref_genders" as gen
  cross join "asha_dev"."main_reference"."ref_support_providers" as sp
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
    lev_gender,
    IsDifferent_PropertyAddress,
    IsDifferent_Room,
    IsDifferent_FirstName,
    IsDifferent_MiddleName,
    IsDifferent_LastName,
    IsDifferent_DateOfBirth,
    IsDifferent_NINumber,
    IsDifferent_CheckinDate,
    IsDifferent_CheckoutDate,
    IsDifferent_NewHBClaim,
    IsDifferent_HBClaimRefNumber,
    IsDifferent_ReferralAgency,
    IsDifferent_GroupedReferralAgency,
    IsDifferent_Age,
    IsDifferent_Gender,
    IsDifferent_Religion,
    IsDifferent_Ethnicity,
    IsDifferent_Nationality,
    IsDifferent_Disability,
    IsDifferent_SexualOrientation,
    IsDifferent_SpokenLanguage,
    IsDifferent_RiskAssessment,
    IsDifferent_LengthOfStay
from lev_table
    );
  
  