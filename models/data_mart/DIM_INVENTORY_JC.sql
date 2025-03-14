{{ config(
    materialized='table',
    unique_key='inventory_id'
) }}

WITH cleaned_inventory_data AS (
    SELECT
        i.inventory_id,
        i.product_id,
        i.warehouse_id,
        i.stock_level,
        TRY_CAST(i.restock_date AS DATE) AS restock_date,
        i.supplier_id,
        i.storage_condition,
        i.inventory_status,
        i.last_audit_date,
        i.reorder_level,
        i.quantity_in_stock,
        i.safety_stock,
        i.average_monthly_demand,
        TRY_CAST(i.last_restock_date AS DATE) AS last_restock_date,
        TRY_CAST(i.next_restock_date AS DATE) AS next_restock_date
    FROM {{ source('de_project', 'inventory_data') }} i
)

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
    safety_stock,
    average_monthly_demand,
    last_restock_date,
    next_restock_date
FROM cleaned_inventory_data