

with latest_support_notes as (
    select
        support_provider_id,
        id,
        support_providers,
        cycle,
        value,
        ingested_at_ts,
        source_file,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from "asha_prod"."history"."hist_support_notes"
    where dbt_valid_to IS NULL
),

latest_support_notes_submissions as (
    select
        support_provider_id,
        id,
        support_providers,
        successful_submission,
        success_pct,
        ingested_at_ts,
        source_file,
        dbt_scd_id,
        dbt_updated_at,
        dbt_valid_from,
        dbt_valid_to
    from "asha_prod"."history"."hist_support_notes_submissions"
    where dbt_valid_to IS NULL
)

select
    a.support_provider_id,
    a.id,
    a.support_providers,
    a.cycle,
    a.value,
    b.successful_submission,
    b.success_pct,
    a.ingested_at_ts,
    a.source_file,
    a.dbt_scd_id,
    a.dbt_updated_at,
    a.dbt_valid_from,
    a.dbt_valid_to
from latest_support_notes a
left join latest_support_notes_submissions b
    on a.support_provider_id = b.support_provider_id