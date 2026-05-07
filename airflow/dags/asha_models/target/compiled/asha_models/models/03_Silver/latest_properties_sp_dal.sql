


select
  *
from "asha_prod"."history"."hist_properties_sp_dal"
where dbt_valid_to is null
order by id asc