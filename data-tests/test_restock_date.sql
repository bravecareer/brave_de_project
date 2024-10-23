-- Ensure last restock date, restock date and next restock date are in order
SELECT last_restock_date, restock_date, next_restock_date
FROM {{ ref('dim_inventory_michael_w') }}
WHERE last_restock_date >= restock_date
OR restock_date >= next_restock_date
