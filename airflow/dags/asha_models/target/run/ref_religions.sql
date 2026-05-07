 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_religions";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_religions" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_religions.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  