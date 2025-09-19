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
        
        -- Core inventory metrics
        quantity_in_stock_clean,
        reorder_level_clean,
        safety_stock_clean,
        average_monthly_demand_clean,
        sales_volume_clean,
        
        -- Add stock level status
        CASE 
            WHEN quantity_in_stock_clean = 0 THEN 'out_of_stock'
            WHEN quantity_in_stock_clean < safety_stock_clean THEN 'Below Safety Stock'
            WHEN quantity_in_stock_clean < reorder_level_clean THEN 'Below Reorder Level'
            WHEN quantity_in_stock_clean > average_monthly_demand_clean * 2 THEN 'Overstocked'
     ELSE 'Normal'
     END AS stock_level_status,

        
        -- Operational dates
        restock_date,
        last_audit_date,
        last_restock_date,
        next_restock_date
          
    FROM {{ ref('stg_inventory_data_yl') }}
    {% if is_incremental() %}
        -- Process only recent rows on incremental runs (2-day sliding window)
        WHERE DATE(last_audit_date) >= CURRENT_DATE() - 2
    {% endif %}
),

inventory_metrics AS (
    SELECT inventory_id,
        product_id,
        warehouse_id,
        quantity_in_stock_clean,
        average_monthly_demand_clean,
        stock_level_status,
        last_audit_date,
        restock_date, 
        next_restock_date,
                -- dbt processing timestamp
        '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at

    FROM inventory_data

)

SELECT * FROM inventory_metrics