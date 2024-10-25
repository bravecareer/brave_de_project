-- Test: test_stock_dates
-- Description: This test checks if the last_restock_date in the dim_inventory_qu
-- table is before the restock_date and if the restock_date is before the next_restock_date.

SELECT
    COUNT(*) AS invalid_rows
FROM
    {{ ref('dim_inventory_qu') }}
WHERE
    last_restock_date >= restock_date
    OR restock_date >= next_restock_date
HAVING
    invalid_rows > 0    