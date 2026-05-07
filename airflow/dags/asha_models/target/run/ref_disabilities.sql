 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_disabilities";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_disabilities" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_disabilities.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  