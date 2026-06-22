 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_prod"."main_reference"."ref_sexual_orientations";
    -- dbt seed --
    
          COPY "asha_prod"."main_reference"."ref_sexual_orientations" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_sexual_orientations.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  