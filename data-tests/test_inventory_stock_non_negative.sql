-- Ensure stock values are non negative
SELECT
  inventory_id,
  product_id
FROM {{ ref('dim_inventory_sp') }}
WHERE safety_stock < 0 OR
      quantity_in_stock < 0
