 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_support_providers";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_support_providers" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_support_providers.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  