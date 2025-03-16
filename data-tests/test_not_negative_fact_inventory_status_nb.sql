-- Test to ensure stock levels are non-negative
SELECT * FROM {{ ref('fact_inventory_nb') }} 
WHERE stock_level < 0 OR quantity_in_stock < 0;