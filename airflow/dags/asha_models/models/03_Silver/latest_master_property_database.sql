select
  *
from {{ ref('hist_master_property_database') }}
where dbt_valid_to IS NULL