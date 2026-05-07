
  
  create view "asha_dev"."main_bronze"."raw_properties_sp_dal__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_properties_sp_dal/*.parquet')
  );
