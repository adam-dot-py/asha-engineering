 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_tsm_scales";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_tsm_scales" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_tsm_scales.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  