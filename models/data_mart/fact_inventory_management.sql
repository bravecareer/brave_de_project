{{ config(
   materialized='incremental',
   unique_key='inventory_id'
) }}

SELECT 
   inventory_id,
   product_id,
   warehouse_id,
   stock_level,
   reorder_level,
   quantity_in_stock,
   inventory_status
FROM {{ ref('stg_inventory_data') }}
WHERE inventory_status IS NOT NULL
