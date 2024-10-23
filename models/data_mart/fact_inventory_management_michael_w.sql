{{ config(
   materialized='incremental',
   unique_key= ['inventory_id', 'product_id', 'warehouse_id']
) }}


WITH inventory AS (
   SELECT
       i.inventory_id,
       i.product_id,
       i.stock_level,
       i.warehouse_id,
       i.restock_date,
       i.last_restock_date,
       i.next_restock_date,
       i.inventory_status,
       i.reorder_level,
       i.sales_volume,
       i.safety_stock,
       i.average_monthly_demand
   FROM {{ source('de_project', 'inventory_data') }} i
)

SELECT * FROM inventory