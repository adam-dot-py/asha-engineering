
      update "asha_prod"."history"."hist_dal_summary" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_dal_summary__dbt_tmp20260817110050611001" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_prod"."history"."hist_dal_summary" ("support_provider_id", "id", "support_providers", "total_units_per_provider", "properties_with_director_as_landlord", "units_owned_by_support_providers", "leased_units_with_ash_shahada", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."support_provider_id",DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."support_providers",DBT_INTERNAL_SOURCE."total_units_per_provider",DBT_INTERNAL_SOURCE."properties_with_director_as_landlord",DBT_INTERNAL_SOURCE."units_owned_by_support_providers",DBT_INTERNAL_SOURCE."leased_units_with_ash_shahada",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_dal_summary__dbt_tmp20260817110050611001" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  