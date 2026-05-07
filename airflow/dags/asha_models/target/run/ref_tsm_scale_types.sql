 -- noqa: Should accept a string instead of a integer
    
    
    truncate table "asha_dev"."main_reference"."ref_tsm_scale_types";
    -- dbt seed --
    
          COPY "asha_dev"."main_reference"."ref_tsm_scale_types" FROM '/home/asha/airflow/dags/asha_models/seeds/ref_tsm_scale_types.csv' (FORMAT CSV, HEADER TRUE, DELIMITER ',')
        

;
  