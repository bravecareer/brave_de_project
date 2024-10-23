-- Tests if the restock dates are in line
SELECT
  inventory_id,
  product_id
FROM {{ ref('dim_inventory_sp') }}
WHERE last_restock_date > restock_date 
    OR last_restock_date > next_restock_date
    OR restock_date > next_restock_date
