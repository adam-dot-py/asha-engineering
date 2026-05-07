
      update "asha_dev"."history"."hist_property_submissions" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_property_submissions__dbt_tmp_7ec71cd7_1675_4ec8_9582_d7682ef13c7d" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_dev"."history"."hist_property_submissions" ("sr_no", "property_address", "units", "support_providers", "submission_date", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."sr_no",DBT_INTERNAL_SOURCE."property_address",DBT_INTERNAL_SOURCE."units",DBT_INTERNAL_SOURCE."support_providers",DBT_INTERNAL_SOURCE."submission_date",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_property_submissions__dbt_tmp_7ec71cd7_1675_4ec8_9582_d7682ef13c7d" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  