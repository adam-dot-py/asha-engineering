{{config(
  post_hook="COPY {{ this }} TO '/home/asha/airflow/dags/gold/semantic/gold_periods.parquet' (FORMAT PARQUET)"
)}}

SELECT
  CAST(CycleNumber AS STRING) AS CycleNumber,
  CAST(Quarter AS STRING) AS Quarter,
  CAST(Year AS STRING) AS Year
FROM {{ ref('ref_periods') }}