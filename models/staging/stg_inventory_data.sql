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
        COALESCE(inventory_id, -1) as inventory_id,
        COALESCE(product_id, -1) as product_id,
        COALESCE(warehouse_id, -1) as warehouse_id,
        COALESCE(supplier_id, -1) as supplier_id,
        
        -- Inventory metrics
        case 
            when stock_level is null then 0
            when stock_level < 0 then 0 
            else stock_level 
        end as stock_level,
        
        -- Inventory thresholds
        case 
            when reorder_level is null then 0
            when reorder_level < 0 then 0 
            else reorder_level 
        end as reorder_level,
        case 
            when safety_stock is null then 0
            when safety_stock < 0 then 0 
            else safety_stock 
        end as safety_stock,
        
        -- Dates
        COALESCE(restock_date, CURRENT_DATE()) as restock_date,
        COALESCE(last_audit_date, CURRENT_DATE()) as last_audit_date,
        COALESCE(last_restock_date, CURRENT_DATE()) as last_restock_date,
        COALESCE(next_restock_date, CURRENT_DATE()) as next_restock_date,
        
        -- Status and conditions
        COALESCE(storage_condition, 'UNKNOWN') as storage_condition,
        COALESCE(inventory_status, 'UNKNOWN') as inventory_status,
        
        -- Additional metrics
        COALESCE(rating, '0') as rating,
        case 
            when sales_volume is null then 0
            when sales_volume < 0 then 0 
            else sales_volume 
        end as sales_volume,
        COALESCE(weight, '0') as weight,
        COALESCE(discounts, '0') as discounts,
        case 
            when average_monthly_demand is null then 0
            when average_monthly_demand < 0 then 0 
            else average_monthly_demand 
        end as average_monthly_demand,
        
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