
  
  create view "asha_dev"."main_bronze"."raw_remittances__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_remittances/*.parquet')
  );
