SELECT
    COUNT(*) AS invalid_rows
FROM
    {{ ref('dim_inventory_qu') }}
WHERE
    last_restock_date >= restock_date
    OR restock_date >= next_restock_date
HAVING
    invalid_rows > 0    