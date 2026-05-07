
  
    
    

    create  table
      "asha_prod"."main_gold"."gold_piop__dbt_tmp"
  
    as (
      

SELECT
    a.cycle,
    a.paid_remittances,
    a.received_remittances,
    a.percentage_differences,
FROM "asha_prod"."main_silver"."latest_piop" a
    );
  
  