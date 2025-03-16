-- Test for NOT NULL constraints
SELECT * FROM {{ ref('fact_inventory_nb') }}  
WHERE inventory_id IS NULL OR product_id IS NULL OR warehouse_id IS NULL OR restock_date IS NULL;

