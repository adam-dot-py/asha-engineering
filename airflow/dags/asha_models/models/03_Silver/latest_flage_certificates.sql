select
  *
from {{ ref('hist_flage_certificates') }}
where dbt_valid_to IS NULL