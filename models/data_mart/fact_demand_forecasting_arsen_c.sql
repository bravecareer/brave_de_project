{{ config(
    materialized='incremental',
    unique_key=['inventory_id', 'product_id', 'warehouse_id']
) }}

WITH inventory_data AS (
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
        inv.sales_volume,
        inv.safety_stock,
        inv.average_monthly_demand,
        inv.last_restock_date,
        inv.next_restock_date
    FROM {{ source('de_project', 'inventory_data') }} inv
),

product_data AS (
    SELECT
        p.product_id,
        p.price
    FROM {{ source('de_project', 'product_data') }} p
),

final AS (
    SELECT
        inv.inventory_id,
        inv.product_id,
        inv.warehouse_id,
        inv.stock_level,
        inv.restock_date,
        inv.inventory_status,
        inv.last_audit_date,
        inv.reorder_level,
        inv.quantity_in_stock,
        inv.sales_volume,
        inv.safety_stock,
        inv.average_monthly_demand,
        inv.last_restock_date,
        inv.next_restock_date,
        p.price AS product_price
    FROM inventory_data inv
    LEFT JOIN product_data p ON inv.product_id = p.product_id
)

SELECT * FROM final