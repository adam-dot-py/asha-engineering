
      update "asha_dev"."history"."hist_properties_sp_dal" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_properties_sp_dal__dbt_tmp_d514550d_1fbd_4841_82e1_703fd54a8e36" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_dev"."history"."hist_properties_sp_dal" ("id", "support_providers", "property_address", "number_of_units", "is_the_landlord_same_as_company_director?_(yes_or_no)", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."id",DBT_INTERNAL_SOURCE."support_providers",DBT_INTERNAL_SOURCE."property_address",DBT_INTERNAL_SOURCE."number_of_units",DBT_INTERNAL_SOURCE."is_the_landlord_same_as_company_director?_(yes_or_no)",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_properties_sp_dal__dbt_tmp_d514550d_1fbd_4841_82e1_703fd54a8e36" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  