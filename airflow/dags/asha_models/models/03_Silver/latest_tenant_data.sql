{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/silver/latest/latest_tenant_data.parquet' (FORMAT PARQUET)"
)}}


select *
from {{ ref('hist_tenant_data') }}
qualify CycleNumberValue = max(CycleNumberValue) over (partition by Tenant_SK)