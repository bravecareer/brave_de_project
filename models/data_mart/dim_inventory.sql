{{ config(
   materialized='view',
   unique_key='inventory_id'
) }}

-- Get inventory data with recent audit records
WITH inventory_data AS (
   SELECT
       i.inventory_id,
       i.reorder_level,
       i.product_id,
       i.quantity_in_stock,
       i.warehouse_id,
       i.rating,
       i.stock_level,
       i.sales_volume,
       i.restock_date,
       i.weight,
       i.supplier_id,
       i.discounts,
       i.storage_condition,
       i.safety_stock,
       i.inventory_status,
       i.average_monthly_demand,
       i.last_audit_date,
       i.last_restock_date,
       i.next_restock_date,
       -- Add calculated fields
       p.price * i.quantity_in_stock as total_inventory_value,
       CASE 
           WHEN i.average_monthly_demand > 0 
           THEN ROUND(i.quantity_in_stock::FLOAT / (i.average_monthly_demand / 30), 1)
           ELSE NULL 
       END as estimated_days_of_inventory
   FROM {{ ref('stg_inventory_data') }} i
   LEFT JOIN {{ ref('dim_product') }} p ON i.product_id = p.product_id
   WHERE last_audit_date >= CURRENT_DATE() - 5 
)

SELECT * FROM inventory_data