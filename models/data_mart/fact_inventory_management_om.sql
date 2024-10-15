{{ config(
   materialized='incremental',
   unique_key=['product_id', 'warehouse_id']
) }}

WITH inventory_data AS (
   SELECT
       i.product_id,
       i.warehouse_id,
       i.stock_level,
       i.restock_date,
       i.last_audit_date,
       i.inventory_status,
       i.safety_stock,
       i.quantity_in_stock,
       i.average_monthly_demand
   FROM {{ source('de_project', 'inventory_data') }} i
),

inventory_summary AS (
   SELECT
      product_id,
      warehouse_id,
      MAX(stock_level) AS current_stock_level,
      MAX(restock_date) AS next_restock_date,
      MAX(inventory_status) AS latest_inventory_status,
      SUM(quantity_in_stock) AS total_quantity_in_stock,
      SUM(safety_stock) AS total_safety_stock,
      AVG(average_monthly_demand) AS avg_monthly_demand
   FROM inventory_data
   GROUP BY product_id, warehouse_id
)

SELECT * FROM inventory_summary
