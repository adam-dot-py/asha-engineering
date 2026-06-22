

SELECT
	a.id,
    a.property_address as PropertyAddress,
    a.rooms as Rooms,
    a.property_count as PropertyCount,
    a.council_tax_band as CouncilTaxBand,
    a.lease_start_date as LeaseStartDate,
    a.lease_end_date as LeaseEndDate,
    a.renewed_lease_start_date as RenewedLeaseStartDate,
    a.renewed_lease_end_date as RenewedLeaseEndDate,
    a.change_of_rs_01062023_renewed_lease_startdate as ChangeOfRs01062023RenewedLeaseStartDate,
    a.change_of_rs_01062023_renewed_lease_enddate as ChangeOfRs01062023RenewedLeaseEndDate,
    a.change_of_rs_02122024_renewed_lease_startdate as ChangeOfRs02122024RenewedLeaseStartDate,
    a.change_of_rs_02122024_renewed_lease_enddate as ChangeOfRs02122024RenewedLeaseEndDate,
    a.annual_lease_cost_updated_02122024 as AnnualLeaseCostUpdated02122024,
    a."1_month_notice_cost" as OneMonthNoticeCost,
    a.comments as Comments,
    a."3_month_notice_cost" as ThreeMonthNoticeCost,
    a.ingested_at_ts,
    a.source_file,
    a.dbt_scd_id,
    a.dbt_updated_at,
    a.dbt_valid_from,
    a.dbt_valid_to,
    a.support_provider_id,
	a.support_providers as original_support_providers,
	r.support_providers as SupportProviders
from "asha_prod"."main_silver"."latest_lease_database" a
left join lateral (
	select
		s.support_providers
	from "asha_prod"."main_reference"."ref_support_providers" s
	order by
		case
			when lower(trim(a.support_providers)) = lower(trim(s.support_providers)) then 0
			else levenshtein(lower(trim(a.support_providers)), lower(trim(s.support_providers)))
		end,
		s.support_providers
	limit 1
) r on true