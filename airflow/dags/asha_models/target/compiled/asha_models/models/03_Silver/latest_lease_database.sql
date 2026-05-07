


select
  *
from "asha_prod"."history"."hist_lease_database"
where dbt_valid_to is null
order by id asc