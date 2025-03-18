
  create or replace   view DEV.RDL.dim_pets
  
   as (
    WITH cust_src AS (
    SELECT
        data:detail:customer:email::STRING AS email,
        data:detail:pets                   AS pets_array,
        ingest_date
    FROM dev.rdl.orders
),
flattened_pets AS (
    SELECT 
        d.email,
        pet.value:name::STRING              AS pet_name,
        pet.value:breed_value::STRING       AS breed,
        pet.value:dob::DATE                 AS pet_dob,
        pet.value:gender::STRING            AS pet_gender,
        pet.value:species::STRING           AS pet_species,
        pet.value:pedigree_type::STRING     AS pedigree_type,
        pet.value:spayed_neutered::BOOLEAN  AS spayed_neutered,
        pet.value:postcode::STRING          AS pet_postcode,
        d.ingest_date
    FROM cust_src d,
    LATERAL FLATTEN(input => d.pets_array) AS pet
),
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY email, pet_dob, pet_name ORDER BY ingest_date DESC) AS rnk
    FROM flattened_pets
)
SELECT
    ROW_NUMBER() OVER(ORDER BY d.email, d.pet_dob, d.pet_name)                                      AS pet_sk,  
    d.email || '-' || COALESCE(d.pet_name, 'unknown') || '-' || COALESCE(d.pet_dob, '1900-01-01')   AS pet_nk,
    c.customer_sk                                                                                   AS customer_sk,
    d.pet_name                                                                                AS pet_name,
    d.breed                                                                                   AS breed,
    d.pet_dob                                                                                 AS pet_dob,
    d.pet_gender                                                                              AS pet_gender,
    d.pet_species                                                                             AS pet_species,
    d.pedigree_type                                                                           AS pedigree_type,
    d.spayed_neutered                                                                         AS spayed_neutered,
    d.pet_postcode                                                                            AS pet_postcode
FROM 
    dedup AS d
    INNER JOIN DEV.RDL.dim_customers AS c ON (d.email=c.customer_nk)
WHERE
    rnk = 1
  );

