 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_risk_assessments";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_risk_assessments" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_risk_assessments.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  