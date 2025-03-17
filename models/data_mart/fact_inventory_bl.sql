{{ config(
   materialized='table',
   unique_key='inventory_id'
) }}


WITH inventory_data AS (
   SELECT
       i.inventory_id,
       i.product_id,
       p.product_name,
       i.supplier_id,
       i.warehouse_id,
       i.storage_condition,
       i.stock_level,
       i.quantity_in_stock,
       i.reorder_level,
       i.safety_stock,
       i.inventory_status,
       i.sales_volume,
       i.average_monthly_demand,
       i.restock_date,
       i.last_restock_date,
       i.next_restock_date,
       i.last_audit_date
   FROM {{ ref('stg_inventory_data_bl') }} i
   LEFT JOIN {{ ref('stg_product_data_bl') }} p
   ON i.product_id = p.product_id
)


SELECT * FROM inventory_data