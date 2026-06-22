 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_prod"."main_reference"."ref_tsm_headers";
    -- dbt seed --
    
          COPY "asha_prod"."main_reference"."ref_tsm_headers" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_tsm_headers.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  