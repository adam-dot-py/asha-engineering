{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/history/hist_tenant_data.parquet' (FORMAT PARQUET)"
)}}

WITH history AS (
    SELECT *,
        hash(Tenant_SK,
        PropertyAddress,
        Room,
        FirstName,
        LastName,
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
        LengthOfStay) AS row_hash,
        LAG(hash(
            Tenant_SK,
            PropertyAddress,
            Room,
            FirstName,
            LastName,
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
            LengthOfStay
            ))
            OVER (PARTITION BY Tenant_SK ORDER BY CycleNumberValue DESC) AS prev_row_hash
    FROM {{ ref('stg_tenant_data') }}
)

SELECT 
  *,
   CASE
       WHEN prev_row_hash IS NULL THEN 'New Record'
       WHEN row_hash != prev_row_hash THEN 'Changed'
       ELSE 'Unchanged'
    END AS record_status,
    CASE
       WHEN prev_row_hash IS NULL THEN NULL
       WHEN row_hash != prev_row_hash THEN current_date - interval '1 day'
       ELSE NULL
    END AS dbt_valid_to
FROM history