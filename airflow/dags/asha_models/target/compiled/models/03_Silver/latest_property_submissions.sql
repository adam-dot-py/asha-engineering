


select
  *
from "asha_dev"."history"."hist_property_submissions"
where dbt_valid_to is null
order by sr_no asc