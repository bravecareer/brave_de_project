-- Test to ensure stock levels are non-negative
SELECT * FROM {{ ref('stg_inventory_status_nb') }}  
WHERE stock_level < 0 OR quantity_in_stock < 0;
