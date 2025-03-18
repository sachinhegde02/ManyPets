{{ config(
    unique_key='pet_nk',  
    incremental_strategy='merge'  
) }}

WITH stg_data AS (
    SELECT
        customer_email,
        pet_dob,
        pet_name,
        pet_breed,
        pet_gender,
        pet_species,
        pedigree_type,
        spayed_neutered,
        pet_postcode,
        pet_value,
        ingest_date
    FROM {{ ref('stg_orders') }}
    
    {% if is_incremental() %}
        WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})
    {% endif %}
),

dedup AS (
    SELECT
        customer_email,
        pet_dob,
        pet_name,
        pet_breed,
        pet_gender,
        pet_species,
        pedigree_type,
        spayed_neutered,
        pet_postcode,
        pet_value,
        ingest_date,
        ROW_NUMBER() OVER(PARTITION BY customer_email, pet_dob, pet_name, pet_breed ORDER BY ingest_date DESC) AS rnk
    FROM stg_data
)
SELECT
    d.customer_email ||'|'||d.pet_name || '|' || d.pet_dob || '|' || d.pet_breed      AS pet_nk,  
    {{ dbt_utils.generate_surrogate_key(['pet_nk']) }}                                AS pet_sk,  
    d.pet_name,
    d.pet_dob,
    d.pet_breed,
    d.pet_gender,
    d.pet_species,
    d.pedigree_type,
    d.spayed_neutered,
    d.pet_postcode,
    d.pet_value,
    d.ingest_date,
    c.customer_sk
FROM dedup AS d
    INNER JOIN {{ref("dim_customers")}} AS c ON (c.customer_nk=d.customer_email)
WHERE d.rnk = 1
