

select
  *
from "asha_prod"."history"."hist_clawbacks"
where dbt_valid_to IS NULL