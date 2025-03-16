-- Test for NOT NULL constraints
SELECT * FROM {{ ref('stg_inventory_status_nb') }}  
WHERE inventory_id IS NULL OR product_id IS NULL OR warehouse_id IS NULL;
