select
  *
from {{ ref('hist_voids') }}
where dbt_valid_to IS NULL