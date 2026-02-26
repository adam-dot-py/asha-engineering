{% macro merge_tenant_data() %}

  {% set merge_sql %}
    MERGE INTO history.hist_tenant_data AS target
    USING {{ ref('stg_tenant_data') }} AS source
    ON 
    target.Tenant_SK = source.Tenant_SK 
    AND target.CycleNumberValue = source.CycleNumberValue 
    WHEN MATCHED AND (
    target.PropertyAddress <> source.PropertyAddress
    OR target.Room <> source.Room
    OR target.FirstName <> source.FirstName
    OR target.MiddleName <> source.MiddleName
    OR target.LastName <> source.LastName
    OR target.DateOfBirth <> source.DateOfBirth
    OR target.NINumber <> source.NINumber
    OR target.CheckinDate <> source.CheckinDate
    OR target.CheckoutDate <> source.CheckoutDate
    OR target.NewHBClaim <> source.NewHBClaim
    OR target.HBClaimRefNumber <> source.HBClaimRefNumber
    OR target.ReferralAgency <> source.ReferralAgency
    OR target.GroupedReferralAgency <> source.GroupedReferralAgency
    OR target.Age <> source.Age
    OR target.Gender <> source.Gender
    OR target.Religion <> source.Religion
    OR target.Ethnicity <> source.Ethnicity
    OR target.Nationality <> source.Nationality
    OR target.Disability <> source.Disability
    OR target.SexualOrientation <> source.SexualOrientation
    OR target.SpokenLanguage <> source.SpokenLanguage
    OR target.RiskAssessment <> source.RiskAssessment
    OR target.LengthOfStay <> source.LengthOfStay
    OR target.CycleNumber <> source.CycleNumber
    OR target.Source <> source.Source
    OR target.ExtractedProviderName <> source.ExtractedProviderName
    OR target.ProviderName <> source.ProviderName
    ) THEN UPDATE SET
        dbt_valid_to = CURRENT_DATE - INTERVAL '1 day'
    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET
        dbt_valid_to = CURRENT_DATE - INTERVAL '1 day'
    WHEN NOT MATCHED BY TARGET THEN INSERT (
    record_id,
    Tenant_SK,
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
    dbt_valid_to
    ) VALUES (
    nextval('tenant_record_seq'),
    source.Tenant_SK,
    source.PropertyAddress,
    source.Room,
    source.FirstName,
    source.MiddleName,
    source.LastName,
    source.DateOfBirth,
    source.NINumber,
    source.CheckinDate,
    source.CheckoutDate,
    source.NewHBClaim,
    source.HBClaimRefNumber,
    source.ReferralAgency,
    source.GroupedReferralAgency,
    source.Age,
    source.Gender,
    source.Religion,
    source.Ethnicity,
    source.Nationality,
    source.Disability,
    source.SexualOrientation,
    source.SpokenLanguage,
    source.RiskAssessment,
    source.LengthOfStay,
    source.CycleNumber,
    source.CycleNumberValue,
    source.Source,
    source.ExtractedProviderName,
    source.ProviderName,
    NULL
    )
    RETURNING merge_action, *;
    
  {% endset %}

{% do run_query(merge_sql) %}

{% endmacro %}