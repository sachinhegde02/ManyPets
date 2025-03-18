
  
    

        create or replace transient table DEV.odl.dim_customers
         as
        (
WITH stg_data AS (
    SELECT
        customer_email,
        customer_name,
        customer_dob,
        customer_phone,
        customer_address,
        customer_city,
        customer_country,
        customer_postcode,
        ingest_date  
    FROM DEV.staging.stg_orders
    
    
),
dedup AS (
    SELECT
        customer_email,
        customer_name,
        customer_dob,
        customer_phone,
        customer_address,
        customer_city,
        customer_country,
        customer_postcode,
        ingest_date,
        ROW_NUMBER() OVER (PARTITION BY customer_email ORDER BY ingest_date DESC) AS rnk
    FROM stg_data
)
SELECT
    customer_email                                                          AS customer_nk, 
    md5(cast(coalesce(cast(customer_nk as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))                 AS customer_sk,
    customer_name,
    customer_dob,
    customer_phone,
    customer_address,
    customer_city,
    customer_country,
    customer_postcode,
    ingest_date
FROM dedup
WHERE rnk = 1
        );
      
  