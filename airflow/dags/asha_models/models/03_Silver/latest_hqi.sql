{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_hqi.parquet' (FORMAT PARQUET)"
)}}

select
    support_provider_id,
    id,
    support_providers,
    inspection_month,
    number_of_units_covered as NumberofUnitsCovered,
    number_of_housing_quality_issues_identified_during_inspection_ as NumberofHousingQualityIssuesIdentifiedDuringInspection,
    high_priority_ as HighPriority,
    _low_priority_ as LowPriority,
    "non-emergency_(other)" as NonEmergencyOther,
    number_of_housing_quality_issues_resolved_within_given_time_frame as NumberofHousingQualityIssuesResolvedwithinGivenTimeFramePct,
    ingested_at_ts,
    source_file,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
from {{ ref('hist_hqi') }}
where dbt_valid_to IS NULL