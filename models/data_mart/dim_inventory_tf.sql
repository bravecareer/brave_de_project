{{ config(
   materialized='incremental',
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
       -- Add calculated fields
       p.price as unit_price,
       i.last_audit_date
   FROM {{ ref('stg_inventory_data_tf') }} i
   LEFT JOIN {{ ref('stg_product_data_tf') }} p ON i.product_id = p.product_id
   {% if is_incremental() %}
   -- Only process new or updated inventory in incremental runs
   WHERE i.last_audit_date >= CURRENT_DATE() - 5
      OR i.last_restock_date >= CURRENT_DATE() - 2
   {% endif %}
)

SELECT * FROM inventory_data