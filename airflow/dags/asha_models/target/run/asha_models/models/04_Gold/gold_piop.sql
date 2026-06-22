
  
    
    

    create  table
      "asha_prod"."main_gold"."gold_piop__dbt_tmp"
  
    as (
      

SELECT
    a.cycle as Cycle,
    a.paid_remittances as PaidRemittances,
    a.received_remittances as ReceivedRemittances,
    a.percentage_differences as PercentageDifferences
FROM "asha_prod"."main_silver"."latest_piop" a
    );
  
  