
  
  create view "asha_prod"."main_staging"."stg_tsm_sea_responses__dbt_tmp" as (
    select * from "asha_prod"."main_bronze"."raw_tsm_sea_responses"
  );
