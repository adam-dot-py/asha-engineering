

SELECT
  CAST(CycleNumber AS STRING) AS CycleNumber,
  CAST(Quarter AS STRING) AS Quarter,
  CAST(Year AS STRING) AS Year
FROM "asha_prod"."main_reference"."ref_periods"