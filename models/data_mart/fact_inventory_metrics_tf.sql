{{ config(
    materialized='incremental',
    unique_key=['product_id', 'warehouse_id', 'date_key'],
    incremental_strategy='delete+insert'
) }}

WITH inventory_daily AS (
    SELECT
        i.product_id,
        i.warehouse_id,
        i.inventory_id,
        DATE(i.dbt_loaded_at) as date_key,
        i.stock_level as current_stock_level,
        i.average_monthly_demand
    FROM {{ ref('stg_inventory_data_tf') }} i
    {% if is_incremental() %}
    WHERE DATE(i.dbt_loaded_at) >= CURRENT_DATE() - 5
    {% endif %}
)

-- Only include raw metrics, move calculations to BI layer
SELECT
    product_id,
    warehouse_id,
    date_key,
    current_stock_level,
    inventory_id,
    average_monthly_demand
FROM inventory_daily
