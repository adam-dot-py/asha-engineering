select
  *
from {{ ref('hist_clawbacks') }}
where dbt_valid_to IS NULL