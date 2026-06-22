


select *
from "asha_dev"."main_silver"."hist_tenant_data"
qualify CycleNumberValue = max(CycleNumberValue) over (partition by Tenant_SK)