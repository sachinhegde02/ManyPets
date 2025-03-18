select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_email
from DEV.staging.stg_orders
where customer_email is null



      
    ) dbt_internal_test