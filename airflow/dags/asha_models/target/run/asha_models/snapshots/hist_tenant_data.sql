
      update "asha_dev"."history"."hist_tenant_data" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_tenant_data__dbt_tmp20260224000154501394" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_dev"."history"."hist_tenant_data" ("Tenant_SK", "PropertyAddress", "Room", "FirstName", "MiddleName", "LastName", "DateOfBirth", "NINumber", "CheckinDate", "CheckoutDate", "NewHBClaim", "HBClaimRefNumber", "ReferralAgency", "GroupedReferralAgency", "Age", "Gender", "Religion", "Ethnicity", "Nationality", "Disability", "SexualOrientation", "SpokenLanguage", "RiskAssessment", "LengthOfStay", "CycleNumber", "CycleNumberValue", "Source", "ExtractedProviderName", "ProviderName", "LoadDate", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."Tenant_SK",DBT_INTERNAL_SOURCE."PropertyAddress",DBT_INTERNAL_SOURCE."Room",DBT_INTERNAL_SOURCE."FirstName",DBT_INTERNAL_SOURCE."MiddleName",DBT_INTERNAL_SOURCE."LastName",DBT_INTERNAL_SOURCE."DateOfBirth",DBT_INTERNAL_SOURCE."NINumber",DBT_INTERNAL_SOURCE."CheckinDate",DBT_INTERNAL_SOURCE."CheckoutDate",DBT_INTERNAL_SOURCE."NewHBClaim",DBT_INTERNAL_SOURCE."HBClaimRefNumber",DBT_INTERNAL_SOURCE."ReferralAgency",DBT_INTERNAL_SOURCE."GroupedReferralAgency",DBT_INTERNAL_SOURCE."Age",DBT_INTERNAL_SOURCE."Gender",DBT_INTERNAL_SOURCE."Religion",DBT_INTERNAL_SOURCE."Ethnicity",DBT_INTERNAL_SOURCE."Nationality",DBT_INTERNAL_SOURCE."Disability",DBT_INTERNAL_SOURCE."SexualOrientation",DBT_INTERNAL_SOURCE."SpokenLanguage",DBT_INTERNAL_SOURCE."RiskAssessment",DBT_INTERNAL_SOURCE."LengthOfStay",DBT_INTERNAL_SOURCE."CycleNumber",DBT_INTERNAL_SOURCE."CycleNumberValue",DBT_INTERNAL_SOURCE."Source",DBT_INTERNAL_SOURCE."ExtractedProviderName",DBT_INTERNAL_SOURCE."ProviderName",DBT_INTERNAL_SOURCE."LoadDate",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_tenant_data__dbt_tmp20260224000154501394" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  