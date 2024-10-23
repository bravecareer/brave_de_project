-- Ensure no NULL values in critical columns
SELECT
  inventory_id,
  product_id
FROM {{ ref('dim_inventory_sp') }}
WHERE inventory_id IS NULL
   OR product_id IS NULL
   OR warehouse_id IS NULL
