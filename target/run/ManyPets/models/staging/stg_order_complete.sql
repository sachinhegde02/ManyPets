
  
    

        create or replace transient table DEV.staging.stg_order_complete
         as
        (

WITH source_data AS (
    SELECT
        data:detail:order_id::STRING        AS order_id,
        data:detail:intent::STRING          AS intent,
        data:detail:tenant::STRING          AS tenant,
        data:detail:customer_id::STRING     AS customer_id,
        pet.value                           AS pet_id,  
        data:time::TIMESTAMP                AS order_time,
        data:source::STRING                 AS source,
        data:type::STRING                   AS type,
        ingest_date                         AS ingest_date
    FROM dev.rdl.order_complete o,
    LATERAL FLATTEN(input => o.data:detail:pet_ids,OUTER => TRUE) AS pet  

    
)
,
dedup AS (
    SELECT 
        *,
        ROW_NUMBER() OVER ( PARTITION BY order_id, customer_id, pet_id ORDER BY ingest_date DESC ) AS rnk
    FROM source_data
)
SELECT 
    order_id,
    customer_id,
    pet_id,
    intent,
    tenant,
    order_time::DATE            AS order_complete_date,
    order_time                  AS order_complete_timestamp,
    source,
    type,
    ingest_date
FROM dedup
WHERE rnk =1
        );
      
  