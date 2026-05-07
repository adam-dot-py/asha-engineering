
  
  create view "asha_dev"."main_bronze"."raw_lease_terminations__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_lease_terminations/*.parquet')
  );
