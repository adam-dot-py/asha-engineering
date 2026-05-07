 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_ethnicities";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_ethnicities" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_ethnicities.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  