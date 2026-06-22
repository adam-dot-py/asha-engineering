
  
  create view "asha_prod"."main_bronze"."raw_gross_surplus__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_gross_surplus/*.parquet')
  );
