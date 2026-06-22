
  
    
    
      
    

    create  table
      "asha_dev"."main_gold"."gold_periods__dbt_tmp"
  
  (
    CycleNumber TEXT,
    Quarter TEXT,
    Year TEXT
    
    )
 ;
    insert into "asha_dev"."main_gold"."gold_periods__dbt_tmp" 
  (
    
      
      CycleNumber ,
    
      
      Quarter ,
    
      
      Year 
    
  )
 (
      
    select CycleNumber, Quarter, Year
    from (
        

SELECT
  CAST(CycleNumber AS STRING) AS CycleNumber,
  CAST(Quarter AS STRING) AS Quarter,
  CAST(Year AS STRING) AS Year
FROM "asha_dev"."main_reference"."ref_periods"
    ) as model_subq
    );
  
  