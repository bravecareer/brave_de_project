{{ config(
    materialized='incremental',
    unique_key=['inventory_id', 'last_audit_date'],
    incremental_strategy='merge'
) }}

WITH inventory_daily AS (
    SELECT
        i.inventory_id,
        i.product_id,
        i.last_audit_date,
        i.stock_level as current_stock_level,
        i.average_monthly_demand,
        i.stock_level_status
    FROM {{ ref('stg_inventory_data_tf') }} i
    {% if is_incremental() %}
    -- Only process today's data to avoid overwriting historical inventory records
    WHERE DATE(i.dbt_loaded_at) = CURRENT_DATE()
    {% endif %}
)

SELECT
    inventory_id,
    product_id,
    last_audit_date,
    current_stock_level,
    average_monthly_demand,
    stock_level_status
FROM inventory_daily
