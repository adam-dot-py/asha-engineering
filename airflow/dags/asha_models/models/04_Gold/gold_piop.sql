{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_piop.parquet' (FORMAT PARQUET)"
)}}

SELECT
    a.cycle as Cycle,
    a.paid_remittances as PaidRemittances,
    a.received_remittances as ReceivedRemittances,
    a.percentage_differences as PercentageDifferences
FROM {{ ref('latest_piop') }} a