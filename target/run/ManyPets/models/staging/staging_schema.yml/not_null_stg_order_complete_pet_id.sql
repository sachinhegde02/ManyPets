select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pet_id
from DEV.staging.stg_order_complete
where pet_id is null



      
    ) dbt_internal_test