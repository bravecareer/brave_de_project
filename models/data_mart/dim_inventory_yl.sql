{{ config(
    materialized='incremental',
    unique_key='inventory_id',
    incremental_strategy='merge'
) }}

WITH inventory_data AS (
    SELECT
        inventory_id,
        product_id,
        warehouse_id,
        restock_date,
        supplier_id,
        last_audit_date,
        last_restock_date,
        next_restock_date,
        
        -- Cleaned fields from staging
        inventory_status_clean,
        storage_condition_clean,
        stock_level_clean,
        quantity_in_stock_clean,
        reorder_level_clean,
        safety_stock_clean,
        weight_clean,
        
        -- dbt processing timestamp
        '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at
        
    FROM {{ ref('stg_inventory_data_yl') }}
    {% if is_incremental() %}
        -- Process only recent rows on incremental runs (2-day sliding window)
        WHERE DATE(last_audit_date) >= CURRENT_DATE() - 2
    {% endif %}
)

SELECT * FROM inventory_data