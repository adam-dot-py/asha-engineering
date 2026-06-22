
  
  create view "asha_prod"."main_bronze"."raw_support_notes_submissions__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_support_notes_submissions/*.parquet')
  );
