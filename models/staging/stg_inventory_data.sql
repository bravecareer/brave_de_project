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
        {{ safe_numeric('inventory_id', min_value=-1) }} as inventory_id,
        {{ safe_numeric('product_id', min_value=-1) }} as product_id,
        {{ safe_numeric('warehouse_id', min_value=-1) }} as warehouse_id,
        {{ safe_numeric('supplier_id', min_value=-1) }} as supplier_id,
        
        -- Inventory metrics
        {{ safe_numeric('stock_level', min_value=0) }} as stock_level,
        
        -- Inventory thresholds
        {{ safe_numeric('reorder_level', min_value=0) }} as reorder_level,
        {{ safe_numeric('safety_stock', min_value=0) }} as safety_stock,
        
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
        {{ safe_numeric('sales_volume', min_value=0) }} as sales_volume,
        {{ default_value('weight', "'0'") }} as weight,
        {{ default_value('discounts', "'0'") }} as discounts,
        {{ safe_numeric('average_monthly_demand', min_value=0) }} as average_monthly_demand,
        
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