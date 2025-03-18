select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    customer_nk as unique_field,
    count(*) as n_records

from DEV.odl.dim_customers
where customer_nk is not null
group by customer_nk
having count(*) > 1



      
    ) dbt_internal_test