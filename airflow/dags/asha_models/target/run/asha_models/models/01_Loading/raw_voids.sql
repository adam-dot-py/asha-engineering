
  
  create view "asha_prod"."main_bronze"."raw_voids__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_voids/*.parquet')
  );
