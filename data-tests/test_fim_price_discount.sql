-- Testing for the presence if price is greater than discounts in fact_inventory_management_qu
SELECT
    COUNT(*) AS invalid_rows
FROM
    {{ ref('fact_inventory_management_qu') }}
WHERE
    price < discounts
HAVING
    invalid_rows > 0    