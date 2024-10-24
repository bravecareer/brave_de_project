-- Test for no NULL values in critical columns for fact_inventory_management_PS
SELECT
  product_id,
  warehouse_id,
  inventory_status
FROM {{ ref('fact_inventory_management_PS') }}
WHERE product_id IS NULL
  OR warehouse_id IS NULL
  OR inventory_status IS NULL
