{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_piop.parquet' (FORMAT PARQUET)"
)}}

SELECT
    a.cycle,
    a.paid_remittances,
    a.received_remittances,
    a.percentage_differences,
FROM {{ ref('latest_piop') }} a