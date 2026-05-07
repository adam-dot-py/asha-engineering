


select
  *
from "asha_prod"."history"."hist_piop"
where dbt_valid_to is null
order by cycle asc