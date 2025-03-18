select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pet_sk
from DEV.odl.dim_pets
where pet_sk is null



      
    ) dbt_internal_test