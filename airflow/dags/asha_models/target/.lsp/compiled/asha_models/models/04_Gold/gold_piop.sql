

SELECT
    a.cycle as Cycle,
    a.paid_remittances as PaidRemittances,
    a.received_remittances as ReceivedRemittances,
    a.percentage_differences as PercentageDifferences
FROM "asha_dev"."main_silver"."latest_piop" a