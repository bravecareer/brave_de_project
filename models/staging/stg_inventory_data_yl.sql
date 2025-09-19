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
        stock_level,
        restock_date,
        supplier_id,
        storage_condition,
        inventory_status,
        last_audit_date,
        reorder_level,
        quantity_in_stock,
        -- Convert comma decimal separator to period for rating
        CAST(REPLACE(rating, ',', '.') AS DECIMAL(10,6)) AS rating,
        sales_volume,
        -- Convert comma decimal separator to period for weight
        CAST(REPLACE(weight, ',', '.') AS DECIMAL(10,6)) AS weight,
        -- Convert comma decimal separator to period for discounts
        CAST(REPLACE(discounts, ',', '.') AS DECIMAL(10,6)) AS discounts,
        safety_stock,
        average_monthly_demand,
        last_restock_date,
        next_restock_date
    FROM {{ source('de_project', 'inventory_data') }}
    {% if is_incremental() %}
        -- Process only recent rows on incremental runs (2-day sliding window)
        WHERE DATE(last_audit_date) >= CURRENT_DATE() - 2
    {% endif %}
),

cleaned_inventory AS (
    SELECT
        inventory_id,
        product_id,
        warehouse_id,
        restock_date,
        supplier_id,
        last_audit_date,
        last_restock_date,
        next_restock_date,
        
        -- Basic data quality transformations
        CASE 
            WHEN inventory_status IS NULL THEN 'unknown'
            ELSE LOWER(TRIM(inventory_status))
        END AS inventory_status_clean,
        
        CASE 
            WHEN storage_condition IS NULL THEN 'unknown'
            ELSE LOWER(TRIM(storage_condition))
        END AS storage_condition_clean,
        
        -- Handle null values for numeric fields
        COALESCE(stock_level, 0) AS stock_level_clean,
        COALESCE(quantity_in_stock, 0) AS quantity_in_stock_clean,
        COALESCE(reorder_level, 0) AS reorder_level_clean,
        COALESCE(safety_stock, 0) AS safety_stock_clean,
        COALESCE(average_monthly_demand, 0) AS average_monthly_demand_clean,
        COALESCE(sales_volume, 0) AS sales_volume_clean,
        
        -- Handle null values for decimal fields
        COALESCE(rating, 0.0) AS rating_clean,
        COALESCE(weight, 0.0) AS weight_clean,
        COALESCE(discounts, 0.0) AS discounts_clean,
        
        -- Add dbt processing timestamp
        '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at
        
    FROM inventory_data
)

SELECT * FROM cleaned_inventory