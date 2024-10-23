{{ config(
    materialized='incremental',
    unique_key='inventory_id'
) }}

WITH inventory_data AS (
   SELECT
       i.inventory_id,
       i.reorder_level,
       i.product_id,
       i.warehouse_id,
       TO_NUMBER(REPLACE(i.rating, ',', '.'), 3, 2) AS rating,
       i.stock_level,
       i.sales_volume,
       i.restock_date,
       TO_NUMBER(REPLACE(i.weight, ',', '.'), 4, 2) AS weight,
       i.supplier_id,
       TO_NUMBER(REPLACE(i.discounts, ',', '.'), 4, 2) AS discounts,
       i.storage_condition,
       i.safety_stock,
       i.inventory_status,
       i.average_monthly_demand,
       i.last_audit_date AS last_audit_datetime,
       i.last_restock_date,
       i.next_restock_date
   FROM {{ source('de_project', 'inventory_data') }} i
   WHERE i.inventory_id IS NOT NULL
)

SELECT * FROM inventory_data