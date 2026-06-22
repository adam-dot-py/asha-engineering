{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_tenant_data.parquet' (FORMAT PARQUET)"
)}}

with lev_gen_table as (
  select
    latest.*,
    gen.value as lev_gender,
    levenshtein(lower(Gender), lower(gen.value)) as lev_gender_distance
  from {{ ref('latest_tenant_data') }} as latest
  cross join {{ref('ref_genders')}} as gen
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
    lev_gender_distance,
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
    IsDifferent_LengthOfStay,
    IsDifferent_ProviderName,
    LoadDate
from lev_gen_table