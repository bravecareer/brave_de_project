SELECT
    COUNT(*) AS invalid_rows
FROM
    {{ ref('dim_product_qu') }}
WHERE
    manufacturing_date > expiration_date
HAVING
    invalid_rows > 0  