
  
  create view "asha_prod"."main_staging"."stg_tenant_data__dbt_tmp" as (
    select 
    replace(Tenant_SK, ' ', '') as Tenant_SK, -- remove spaces
    PropertyAddress,
    Room,
    FirstName,
    MiddleName,
    LastName,
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
    LoadDate
from "asha_prod"."main_bronze"."raw_tenant_data"
  );
