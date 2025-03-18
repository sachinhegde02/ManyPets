{{ config(
    unique_key='order_nk',  
    incremental_strategy='merge'  
) }} 
WITH fact_data AS (
    SELECT 
        f.order_id,
        f.cessation_date,
        f.affiliate_code,
        f.pet_value,
        f.source,
        f.type,
        f.order_create_date,
        f.order_create_timestamp,
        f.intent,
        f.tenant,
        /*f.event_time,
        f.envelope_version,
        f.detail_version,
        f.experiments,*/
        c.customer_sk,
        p.pet_sk,
        f.ingest_date
    FROM {{ ref("stg_orders") }} f
    LEFT JOIN {{ ref("dim_customers") }} c 
        ON f.customer_email = c.customer_nk
    LEFT JOIN {{ ref("dim_pets") }} p 
        ON f.pet_name = p.pet_name 
        AND f.pet_dob = p.pet_dob
        AND f.pet_breed = p.pet_breed

    {% if is_incremental() %}
        WHERE f.ingest_date > (SELECT MAX(ingest_date) FROM {{ this }})
    {% endif %}
)

, dedup AS (
    SELECT 
        f.*,
        ROW_NUMBER() OVER(PARTITION BY f.order_id, f.customer_sk, f.pet_sk ORDER BY f.ingest_date DESC) AS rnk
    FROM fact_data f
)

SELECT 
     order_id || '|' || customer_sk || '|'|| pet_sk         AS order_nk,
    {{ dbt_utils.generate_surrogate_key(['order_nk']) }}    AS order_sk,
    -- order_id is added as connecting between order and order_complete on order_sk is not feasible due to missing customer_id and pet_id in the dataset.
    order_id                                                AS order_id,
    customer_sk                                             AS customer_sk,
    pet_sk                                                  AS pet_sk,
    order_create_date                                       AS order_create_date,
    order_create_timestamp                                  AS order_create_timestamp,
    cessation_date                                          AS cessation_date,
    COALESCE(affiliate_code, 'unknown')                     AS affiliate_code,
    pet_value                                               AS pet_value,
    source                                                  AS order_source,
    type                                                    AS order_type,
    intent                                                  AS intent,
    tenant                                                  AS tenant,
/*
    event_time,
    envelope_version,
    detail_version,
    intent,
    tenant,
    experiments,
*/
    ingest_date
FROM dedup
WHERE rnk = 1
