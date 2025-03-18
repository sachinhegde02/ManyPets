

WITH cust_src AS (
    SELECT
        data:detail:customer:first_name::STRING      AS first_name,
        data:detail:customer:last_name::STRING       AS last_name,
        data:detail:customer:dob::DATE               AS dob,
        data:detail:customer:email::STRING           AS email,
        data:detail:customer:phone::STRING           AS phone,
        data:detail:customer:address:line1::STRING   AS address_line1,
        data:detail:customer:address:line2::STRING   AS address_line2,
        data:detail:customer:address:city::STRING    AS city,
        data:detail:customer:address:country::STRING AS country,
        data:detail:customer:address:postcode::STRING AS postcode,
        ingest_date
    FROM dev.rdl.orders

    
)

SELECT
    md5(cast(coalesce(cast(email as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) AS customer_sk,  -- Generate surrogate key
    email                                    AS customer_nk,  
    COALESCE(NULLIF(TRIM(COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')), ''), 'unknown') AS name,
    dob                                      AS dob,
    email                                    AS email,
    COALESCE(NULLIF(phone, ''), 'unknown')   AS phone,
    address_line1 || ' ' || address_line2    AS address,
    city                                     AS city,
    country                                  AS country,
    postcode                                 AS postcode
FROM cust_src