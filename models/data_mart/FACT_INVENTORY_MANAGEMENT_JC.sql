{{ config(
    materialized='table',
    unique_key='inventory_id'
) }}

WITH inventory_management AS (
    SELECT
        i.inventory_id,
        i.product_id,
        i.warehouse_id,
        i.stock_level,
        i.quantity_in_stock,
        i.sales_volume,
        i.reorder_level,
        i.safety_stock,
        i.average_monthly_demand
    FROM {{ source('de_project', 'inventory_data') }} i
)

SELECT
    inventory_id,
    product_id,
    warehouse_id,
    stock_level,
    quantity_in_stock,
    sales_volume,
    reorder_level,
    safety_stock,
    average_monthly_demand
FROM inventory_management