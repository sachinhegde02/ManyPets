{{ config(
    unique_key='customer_nk',  
    incremental_strategy='merge'  
) }}
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
    FROM {{ ref('stg_orders') }}
    
    {% if is_incremental() %}
    WHERE ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})
    {% endif %}
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
    {{ dbt_utils.generate_surrogate_key(['customer_nk']) }}                 AS customer_sk,
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
