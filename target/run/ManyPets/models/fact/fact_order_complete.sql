
  
    

        create or replace transient table DEV.odl.fact_order_complete
         as
        ( 

WITH fact_data AS (
    SELECT
        s.order_id,
        s.intent,
        s.tenant,
        s.source,
        s.type,
        s.ingest_date,
        s.order_complete_date,
        s.order_complete_timestamp,
        c.customer_sk,
        p.pet_sk
    FROM DEV.staging.stg_order_complete s
    LEFT JOIN DEV.odl.dim_customers c 
        ON s.customer_id = c.customer_nk   
    LEFT JOIN DEV.odl.dim_pets p 
        ON s.pet_id = p.pet_nk 

               
),
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER ( PARTITION BY order_id ORDER BY ingest_date DESC ) AS rnk
    FROM fact_data
)

SELECT 
    --order_id || '|' || customer_sk || '|' || pet_sk       AS order_nk,
    --customer_sk and pet_sk will be null 
    order_id                                                AS order_nk,
    md5(cast(coalesce(cast(order_nk as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))    AS order_sk,
    -- order_id is added as connecting between order and order_complete on order_sk is not feasible due to missing customer_id and pet_id in the dataset.
    order_id                                                AS order_id,
    customer_sk                                             AS customer_sk,
    pet_sk                                                  AS pet_sk,
    intent                                                  AS intent,
    tenant                                                  AS tenant,
    order_complete_date                                     AS order_complete_date,
    order_complete_timestamp                                AS order_complete_timestamp,
    source                                                  AS order_complete_source,
    type                                                    AS order_complete_type,
    ingest_date                                             AS ingest_date
FROM dedup
WHERE rnk = 1
        );
      
  