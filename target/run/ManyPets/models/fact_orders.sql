
  create or replace   view DEV.RDL.fact_orders
  
   as (
    WITH fact_src AS (
    SELECT 
        o.data:detail:customer:email::STRING    AS email,
        pet.value:name::STRING                  AS pet_name,
        pet.value:dob::DATE                     AS pet_dob,
        o.data:detail:order_id::STRING          AS order_id,
        o.data:detail:effective_date::DATE      AS order_date,
        o.data:detail:cessation_date::DATE      AS cessation_date,
        o.data:detail:affiliate_code::STRING    AS affiliate_code,
        pet.value:value::NUMBER                 AS pet_value
    FROM dev.rdl.orders o,
    LATERAL FLATTEN(input => o.data:detail:pets) AS pet
),

fact_final AS (
    SELECT 
        f.order_id,
        f.order_date,
        f.cessation_date,
        f.affiliate_code,
        f.pet_value,
        c.customer_sk,
        p.pet_sk
    FROM fact_src f
    LEFT JOIN DEV.RDL.dim_customers c ON f.email = c.email
    LEFT JOIN DEV.RDL.dim_pets p ON c.customer_sk = p.customer_sk 
                    AND f.pet_name = p.pet_name 
                    AND f.pet_dob = p.pet_dob
)
SELECT 
    ROW_NUMBER() OVER(ORDER BY customer_sk, pet_sk, order_id) AS order_sk,
    customer_sk,
    pet_sk,
    order_id AS order_nk,
    order_date,
    cessation_date,
    affiliate_code,
    pet_value
FROM fact_final;
  );

