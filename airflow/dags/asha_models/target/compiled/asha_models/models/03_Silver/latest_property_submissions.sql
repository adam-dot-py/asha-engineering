


select
  *
from "asha_prod"."history"."hist_property_submissions"
where dbt_valid_to is null
order by sr_no asc