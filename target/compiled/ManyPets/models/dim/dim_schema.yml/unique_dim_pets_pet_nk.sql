
    
    

select
    pet_nk as unique_field,
    count(*) as n_records

from DEV.odl.dim_pets
where pet_nk is not null
group by pet_nk
having count(*) > 1


