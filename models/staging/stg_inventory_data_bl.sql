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
       i.next_restock_date
   FROM {{ source('de_project', 'inventory_data') }} i
)


SELECT * FROM inventory_data
{% if is_incremental() %}
-- On incremental runs, only process new inventory allowing a few days for late-arriving facts
WHERE last_audit_date >= (SELECT DATEADD(day, -3, max(last_audit_date)) from {{ this }})
{% endif %}