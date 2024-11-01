SELECT *
FROM {{ ref('fact_inventory_management') }}
WHERE inventory_id IS NULL
   OR product_id IS NULL
   OR warehouse_id IS NULL
