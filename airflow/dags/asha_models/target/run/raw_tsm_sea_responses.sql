
  
  create view "asha_dev"."main_bronze"."raw_tsm_sea_responses__dbt_tmp" as (
    select 
  *
from read_xlsx('/mnt/c/Users/ASHA Server/OneDrive - Ash-Shahada Housing Association/source/surveying/tsm-sea-responses.xlsx', 
  sheet='responses', 
  header = true, 
  all_varchar = true,
  normalize_names = true
)
  );
