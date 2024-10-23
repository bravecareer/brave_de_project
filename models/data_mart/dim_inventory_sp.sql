{{ config(
   materialized='incremental',
   unique_key='inventory_id'
) }}


WITH inventory_data AS (
   SELECT
       i.inventory_id,
       i.reorder_level,
       i.product_id,
       i.quantity_in_stock,
       i.warehouse_id,
       round(replace(i.rating, ',','.'),2) as rating,  
       -- i.stock_level, Removing this because it is the same as quantity in stock
       i.sales_volume,
       i.restock_date,
       round(replace(i.weight, ',','.'),2) as weight, --Unknown unit of measurement, transforming "54,56445644" to "54.56"
       i.supplier_id,
       round(replace(i.discounts, ',','.'),2) as discounts, --Unknown unit of measurement, transforming "54,56445644" to "54.56"
       i.storage_condition,
       i.safety_stock,
       i.inventory_status,
       i.average_monthly_demand,
       i.last_audit_date,
       i.last_restock_date,
       i.next_restock_date
   FROM {{ ref('stg_inventory_sp') }} i
)


SELECT * FROM inventory_data