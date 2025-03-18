SELECT
        data:detail:customer:first_name::STRING AS first_name,
        data:detail:customer:last_name::STRING AS last_name,
        data:detail:customer:dob::DATE AS dob,
        data:detail:customer:email::STRING AS email,
        data:detail:customer:phone::STRING AS phone,
        data:detail:customer:address:line1::STRING AS address_line1,
        data:detail:customer:address:line2::STRING AS address_line2,
        data:detail:customer:address:city::STRING AS city,
        data:detail:customer:address:country::STRING AS country,
        data:detail:customer:address:postcode::STRING AS postcode
FROM dev.rdl.orders