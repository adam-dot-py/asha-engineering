 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_spoken_languages";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_spoken_languages" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_spoken_languages.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  