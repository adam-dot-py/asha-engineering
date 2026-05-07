
      update "asha_prod"."history"."hist_leavers" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_leavers__dbt_tmp20260506190033615275" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_prod"."history"."hist_leavers" ("sr_no", "tenant_name", "crn", "leaving_address", "ninumber", "dob", "vacate_date", "date_informed", "notes", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."sr_no",DBT_INTERNAL_SOURCE."tenant_name",DBT_INTERNAL_SOURCE."crn",DBT_INTERNAL_SOURCE."leaving_address",DBT_INTERNAL_SOURCE."ninumber",DBT_INTERNAL_SOURCE."dob",DBT_INTERNAL_SOURCE."vacate_date",DBT_INTERNAL_SOURCE."date_informed",DBT_INTERNAL_SOURCE."notes",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_leavers__dbt_tmp20260506190033615275" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  