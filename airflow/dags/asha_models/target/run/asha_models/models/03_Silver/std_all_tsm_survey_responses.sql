
  
    
    

    create  table
      "asha_prod"."main_silver"."std_all_tsm_survey_responses__dbt_tmp"
  
    as (
      WITH responses AS (

    SELECT *
    FROM "asha_prod"."main_silver"."std_tsm_responses"

    UNION ALL

    SELECT *
    FROM "asha_prod"."main_silver"."std_tsm_sea_responses"

)

SELECT
  ID,
  IDSK,
  lower(SurveySource) AS SurveySource,
  -- fixes the Excel timestamp issue
  TIMESTAMP '1899-12-30'
    + floor(try_cast(StartTime AS DOUBLE)) * INTERVAL 1 DAY
    + (try_cast(StartTime AS DOUBLE) - floor(try_cast(StartTime AS DOUBLE))) * INTERVAL 1 DAY
  AS StartTime,
  EmailAddress,
  Name,
  lower(TP01) AS TP01,
  lower(TP02_TP03_confirm) AS TP02_TP03_confirm,
  lower(TP02) AS TP02,
  lower(TP03) AS TP03,
  lower(TP04) AS TP04,
  lower(TP05) AS TP05,
  lower(TP06) AS TP06,
  lower(TP07) AS TP07,
  lower(TP08) AS TP08,
  lower(TP09_confirm) AS TP09_confirm,
  lower(TP09) AS TP09,
  lower(TP10_confirm) AS TP10_confirm,
  lower(TP10) AS TP10,
  lower(TP11) AS TP11,
  lower(TP12) AS TP12,
  ASHA_NPS,
  ASHA_COMMENT
FROM responses
    );
  
  