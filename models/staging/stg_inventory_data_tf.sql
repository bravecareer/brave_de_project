{{
    config(
        materialized='incremental',
        unique_key='inventory_id',
        incremental_strategy='delete+insert'
    )
}}

-- Staging layer for inventory data
with source as (
    select * from {{ source('de_project', 'inventory_data') }}
),

staged as (
    select
        -- Identifiers
        CASE 
            WHEN inventory_id IS NULL THEN -1
            WHEN inventory_id < -1 THEN -1
            ELSE inventory_id
        END as inventory_id,
        {{ default_value('product_id', "'UNKNOWN'") }} as product_id,
        CASE 
            WHEN warehouse_id IS NULL THEN -1
            WHEN warehouse_id < -1 THEN -1
            ELSE warehouse_id
        END as warehouse_id,
        CASE 
            WHEN supplier_id IS NULL THEN -1
            WHEN supplier_id < -1 THEN -1
            ELSE supplier_id
        END as supplier_id,
        
        -- Inventory metrics
        CASE 
            WHEN stock_level IS NULL THEN 0
            WHEN stock_level < 0 THEN 0
            ELSE stock_level
        END as stock_level,
        
        -- Inventory thresholds
        CASE 
            WHEN reorder_level IS NULL THEN 0
            WHEN reorder_level < 0 THEN 0
            ELSE reorder_level
        END as reorder_level,
        CASE 
            WHEN safety_stock IS NULL THEN 0
            WHEN safety_stock < 0 THEN 0
            ELSE safety_stock
        END as safety_stock,
        
        -- Dates
        {{ default_value('restock_date', 'CURRENT_DATE()') }} as restock_date,
        {{ default_value('last_audit_date', 'CURRENT_DATE()') }} as last_audit_date,
        {{ default_value('last_restock_date', 'CURRENT_DATE()') }} as last_restock_date,
        {{ default_value('next_restock_date', 'CURRENT_DATE()') }} as next_restock_date,
        
        -- Status and conditions
        {{ default_value('storage_condition', "'UNKNOWN'") }} as storage_condition,
        {{ default_value('inventory_status', "'UNKNOWN'") }} as inventory_status,
        
        -- Additional metrics
        {{ default_value('rating', "'0'") }} as rating,
        CASE 
            WHEN sales_volume IS NULL THEN 0
            WHEN sales_volume < 0 THEN 0
            ELSE sales_volume
        END as sales_volume,
        {{ default_value('weight', "'0'") }} as weight,
        {{ default_value('discounts', "'0'") }} as discounts,
        CASE 
            WHEN average_monthly_demand IS NULL THEN 0
            WHEN average_monthly_demand < 0 THEN 0
            ELSE average_monthly_demand
        END as average_monthly_demand,
        
        -- Data quality checks
        case 
            when stock_level is null then 'Invalid: Null Stock'
            when stock_level < 0 then 'Invalid: Negative Stock'
            when stock_level < safety_stock then 'Warning: Below Safety Stock'
            when stock_level < reorder_level then 'Warning: Below Reorder Level'
            else 'Valid'
        end as stock_level_status,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'stg_inventory' as dbt_source

    from source
)

select * from staged