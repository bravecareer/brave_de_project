{{ config(
   materialized='incremental',
   unique_key='inventory_id'
) }}

WITH inventory_cleaned AS (
   SELECT
       inventory_id,
       product_id,
       warehouse_id,
       stock_level,
       restock_date,
       supplier_id,
       storage_condition,
       inventory_status,
       last_audit_date,
       reorder_level,
       quantity_in_stock,
       rating,
       sales_volume,
       weight,
       discounts,
       safety_stock,
       average_monthly_demand,
       last_restock_date,
       next_restock_date
   FROM {{ source('de_project', 'inventory_data') }}
   WHERE product_id IS NOT NULL
     AND quantity_in_stock IS NOT NULL
)

SELECT * FROM inventory_cleaned
{% if is_incremental() %}
    WHERE inventory_id NOT IN (SELECT inventory_id FROM {{ this }})
{% endif %}
