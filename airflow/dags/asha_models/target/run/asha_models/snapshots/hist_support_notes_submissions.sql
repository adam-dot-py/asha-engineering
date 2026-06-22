
      update "asha_prod"."history"."hist_support_notes_submissions" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_support_notes_submissions__dbt_tmp20260622140044859344" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_prod"."history"."hist_support_notes_submissions" ("support_provider_id", "id", "support_providers", "successful_submission", "success_pct", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."support_provider_id",DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."support_providers",DBT_INTERNAL_SOURCE."successful_submission",DBT_INTERNAL_SOURCE."success_pct",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_support_notes_submissions__dbt_tmp20260622140044859344" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  