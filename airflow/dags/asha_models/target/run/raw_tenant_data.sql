
  
  create view "asha_dev"."main_bronze"."raw_tenant_data__dbt_tmp" as (
    select * from read_parquet('/home/asha/airflow/dags/bronze/raw/raw_tenant_data/*.parquet')
  );
