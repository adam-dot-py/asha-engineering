
      update "asha_prod"."history"."hist_piop" as DBT_INTERNAL_TARGET
    set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    from "hist_piop__dbt_tmp20260622140043055105" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_scd_id::text = DBT_INTERNAL_TARGET.dbt_scd_id::text
      and DBT_INTERNAL_SOURCE.dbt_change_type::text in ('update'::text, 'delete'::text)
      
        and DBT_INTERNAL_TARGET.dbt_valid_to is null;
      

    insert into "asha_prod"."history"."hist_piop" ("cycle", "paid_remittances", "received_remittances", "percentage_differences", "ingested_at_ts", "source_file", "dbt_updated_at", "dbt_valid_from", "dbt_valid_to", "dbt_scd_id")
    select DBT_INTERNAL_SOURCE."cycle",DBT_INTERNAL_SOURCE."paid_remittances",DBT_INTERNAL_SOURCE."received_remittances",DBT_INTERNAL_SOURCE."percentage_differences",DBT_INTERNAL_SOURCE."ingested_at_ts",DBT_INTERNAL_SOURCE."source_file",DBT_INTERNAL_SOURCE."dbt_updated_at",DBT_INTERNAL_SOURCE."dbt_valid_from",DBT_INTERNAL_SOURCE."dbt_valid_to",DBT_INTERNAL_SOURCE."dbt_scd_id"
    from "hist_piop__dbt_tmp20260622140043055105" as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type::text = 'insert'::text;


  