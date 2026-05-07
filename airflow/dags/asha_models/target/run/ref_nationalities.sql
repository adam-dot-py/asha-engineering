 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_nationalities";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_nationalities" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_nationalities.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  