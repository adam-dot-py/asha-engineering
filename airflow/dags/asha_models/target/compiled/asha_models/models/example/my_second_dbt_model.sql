-- Use the `ref` function to select from other models

select *
from "asha_dev"."main"."my_first_dbt_model"
where id = 1