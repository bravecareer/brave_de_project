{{ config(
   materialized='view',
   unique_key='inventory_id'
) }}

-- Get inventory data with recent audit records
WITH inventory_data AS (
   SELECT
       i.inventory_id,
       i.product_id,
       i.warehouse_id,
       i.supplier_id,
       i.storage_condition,
       i.safety_stock as safety_stock_level,
       i.reorder_level as restock_point,
       i.average_monthly_demand,
       -- Add calculated fields
       p.price as unit_price
   FROM {{ ref('stg_inventory_data') }} i
   LEFT JOIN {{ ref('stg_product_data') }} p ON i.product_id = p.product_id
   WHERE i.last_audit_date >= CURRENT_DATE() - 5 
)

SELECT * FROM inventory_data