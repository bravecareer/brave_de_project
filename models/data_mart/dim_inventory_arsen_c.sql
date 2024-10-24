{{ config(
   materialized='incremental',
   unique_key='inventory_id'
) }}

WITH inventory_data AS (
   SELECT
      inv.inventory_id,
      inv.product_id,
      inv.warehouse_id,
      inv.stock_level,
      inv.restock_date,
      inv.supplier_id,
      inv.storage_condition,
      inv.inventory_status,
      CAST(inv.last_audit_date AS DATE) AS last_audit_date,
      inv.reorder_level,
      inv.quantity_in_stock,
      --inv.rating,
      inv.sales_volume,
      --inv.weight,
      --inv.discounts,
      inv.safety_stock,
      inv.average_monthly_demand,
      inv.last_restock_date,
      inv.next_restock_date
   FROM {{ source('de_project', 'inventory_data') }} inv
   WHERE inv.inventory_id IS NOT NULL
)

SELECT * FROM inventory_data