 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_prod"."main_reference"."ref_genders";
    -- dbt seed --
    
          COPY "asha_prod"."main_reference"."ref_genders" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_genders.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  