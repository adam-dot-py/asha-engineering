
  
  create view "asha_dev"."main_bronze"."raw_master_property_database__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_master_property_database/*.parquet')
  );
