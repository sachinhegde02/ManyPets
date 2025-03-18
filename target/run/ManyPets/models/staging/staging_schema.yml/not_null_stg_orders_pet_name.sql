select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pet_name
from DEV.staging.stg_orders
where pet_name is null



      
    ) dbt_internal_test