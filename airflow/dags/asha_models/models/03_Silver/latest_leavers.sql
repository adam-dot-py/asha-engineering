select
  *
from {{ ref('hist_leavers') }}
where dbt_valid_to IS NULL