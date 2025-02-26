{{ config(
    materialized='incremental',
    unique_key=['product_id', 'warehouse_id', 'date_key'],
    incremental_strategy='delete+insert'
) }}

WITH inventory_daily AS (
    SELECT
        i.product_id,
        i.warehouse_id,
        DATE(i.last_updated) as date_key,
        i.current_stock_level,
        i.safety_stock_level,
        i.restock_point,
        i.average_monthly_demand,  
        p.price as unit_price
    FROM {{ ref('dim_inventory') }} i
    LEFT JOIN {{ ref('dim_product') }} p ON i.product_id = p.product_id
    {% if is_incremental() %}
    WHERE DATE(i.last_updated) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Calculate daily inventory metrics
inventory_metrics AS (
    SELECT
        product_id,
        warehouse_id,
        date_key,
        current_stock_level,
        safety_stock_level,
        restock_point,
        unit_price,
        -- Inventory value
        current_stock_level * unit_price as total_inventory_value,
        -- Stock status indicators
        CASE 
            WHEN current_stock_level <= safety_stock_level THEN 'Below Safety Stock'
            WHEN current_stock_level <= restock_point THEN 'Below Restock Point'
            ELSE 'Adequate'
        END as stock_status,
        -- Days of inventory calculation using average daily demand
        CASE 
            WHEN average_monthly_demand > 0 
            THEN ROUND(current_stock_level::FLOAT / (average_monthly_demand / 30), 1)
            ELSE NULL 
        END as estimated_days_of_inventory
    FROM inventory_daily
)

SELECT * FROM inventory_metrics
