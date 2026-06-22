
      update "asha_prod"."history"."hist_hqi" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_hqi__dbt_tmp20260622140040900592" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_prod"."history"."hist_hqi" ("support_provider_id", "id", "support_providers", "inspection_month", "number_of_units_covered", "number_of_housing_quality_issues_identified_during_inspection_", "high_priority_", "_low_priority_", "non-emergency_(other)", "number_of_housing_quality_issues_resolved_within_given_time_frame", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."support_provider_id",DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."support_providers",DBT_INTERNAL_SOURCE."inspection_month",DBT_INTERNAL_SOURCE."number_of_units_covered",DBT_INTERNAL_SOURCE."number_of_housing_quality_issues_identified_during_inspection_",DBT_INTERNAL_SOURCE."high_priority_",DBT_INTERNAL_SOURCE."_low_priority_",DBT_INTERNAL_SOURCE."non-emergency_(other)",DBT_INTERNAL_SOURCE."number_of_housing_quality_issues_resolved_within_given_time_frame",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_hqi__dbt_tmp20260622140040900592" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  