select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select customer_sk
from DEV.odl.dim_customers
where customer_sk is null



      
    ) dbt_internal_test