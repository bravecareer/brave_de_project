{{ config(
    materialized='incremental',
    unique_key=['inventory_id', 'product_id', 'warehouse_id']
) }}

WITH inventory_stock_levels AS (
    SELECT
        inv.inventory_id,
        inv.product_id,
        inv.warehouse_id,
        inv.stock_level,
        inv.restock_date,
        inv.inventory_status,
        CAST(inv.last_audit_date AS DATE) AS last_audit_date,
        inv.reorder_level,
        inv.quantity_in_stock,
        inv.safety_stock,
        inv.last_restock_date,
        inv.next_restock_date
    FROM {{ source('de_project', 'inventory_data') }} inv
)

SELECT * FROM inventory_stock_levels