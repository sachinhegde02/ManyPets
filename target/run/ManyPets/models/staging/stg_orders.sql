
  
    

        create or replace transient table DEV.staging.stg_orders
         as
        (

WITH source_data AS (
    SELECT
        data:detail:order_id::STRING                    AS order_id,
        data:detail:customer:email::STRING              AS email,
        data:detail:customer:first_name::STRING         AS first_name,
        data:detail:customer:last_name::STRING          AS last_name,
        data:detail:customer:dob::DATE                  AS dob,
        data:detail:customer:phone::STRING              AS phone,
        data:detail:customer:address:line1::STRING      AS address_line1,
        data:detail:customer:address:line2::STRING      AS address_line2,
        data:detail:customer:address:city::STRING       AS city,
        data:detail:customer:address:country::STRING    AS country,
        data:detail:customer:address:postcode::STRING   AS postcode,
        pet.value:name::STRING                          AS pet_name,
        pet.value:dob::DATE                             AS pet_dob,
        pet.value:breed_value::STRING                   AS pet_breed,
        pet.value:gender::STRING                        AS pet_gender,
        pet.value:species::STRING                       AS pet_species,
        pet.value:pedigree_type::STRING                 AS pedigree_type,
        pet.value:spayed_neutered::BOOLEAN              AS spayed_neutered,
        pet.value:postcode::STRING                      AS pet_postcode,
        pet.value:value::NUMBER                         AS pet_value,
        data:detail:effective_date::DATE                AS order_effective_date,
        data:detail:cessation_date::DATE                AS cessation_date,
        data:detail:affiliate_code::STRING              AS affiliate_code,
        data:source::STRING                             AS source,
        data:type::STRING                               AS type,
        data:detail:intent::STRING                      AS intent,
        data:detail:tenant::STRING                      AS tenant,
        data:time::TIMESTAMP                            AS order_time,
        ingest_date
    FROM dev.rdl.orders o,
    LATERAL FLATTEN(input => o.data:detail:pets,OUTER => TRUE) AS pet

    
),
dedup AS
(
    SELECT
        *
        ,ROW_NUMBER() OVER(PARTITION BY order_id, email, pet_dob, pet_name, pet_breed  ORDER BY ingest_date DESC) AS rnk
    FROM source_data
)
SELECT 
    order_id ,
    email                                                       AS customer_email,
    COALESCE(NULLIF(TRIM(COALESCE(first_name, '') || ' ' || 
            COALESCE(last_name, '')), ''), 'unknown')           AS customer_name,
    dob                                                         AS customer_dob,
    COALESCE(phone,'unknown')                                   AS customer_phone,
    address_line1 || ' ' || address_line2                       AS customer_address,
    city                                                        AS customer_city,
    country                                                     AS customer_country,
    postcode                                                    AS customer_postcode,
    pet_dob,
    pet_name,
    pet_breed,
    pet_gender,
    pet_species,
    pedigree_type,
    spayed_neutered,
    pet_postcode,
    pet_value,
    order_time::DATE                                            AS order_create_date,
    order_time                                                  AS order_create_timestamp,
    cessation_date,
    affiliate_code,
    source,
    type,
    intent,
    tenant,
    order_effective_date,
    ingest_date
FROM dedup
WHERE 
    rnk =1
        );
      
  