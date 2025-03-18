select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select pet_nk
from DEV.odl.dim_pets
where pet_nk is null



      
    ) dbt_internal_test