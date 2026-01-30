select
  *
from {{ ref('hist_dal_summary') }}
where dbt_valid_to IS NULL