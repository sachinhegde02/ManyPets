select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select ingest_date
from DEV.staging.stg_order_complete
where ingest_date is null



      
    ) dbt_internal_test