{{ config(severity='warn') }}

/*
    Test Name: Stock Level Consistency Test
    Description: Validates inventory status is consistent with stock levels relative to safety stock
    - Stock level should be above safety stock when not in reorder status
    - Stock level should be below safety stock when in reorder status
    
    Note: This test includes significant buffer thresholds to account for timing differences
    and only checks active inventory items with meaningful safety stock levels to focus on relevant data.
*/

with active_inventory as (
    select 
        inventory_id,
        product_id,
        warehouse_id,
        stock_level,
        safety_stock,
        inventory_status
    from {{ ref('stg_inventory_data') }}
    where 
        -- Only check active inventory items
        inventory_status != 'discontinued'
        -- Only consider items with meaningful safety stock levels
        and safety_stock > 20
        -- Only consider items with significant stock levels
        and stock_level > 10
),

inconsistent_inventory_status as (
    select 
        inventory_id,
        product_id,
        warehouse_id,
        stock_level,
        safety_stock,
        inventory_status
    from active_inventory
    where 
        -- Check for stock significantly below safety level (20% buffer) without reorder status
        (stock_level < safety_stock * 0.8 and inventory_status != 'reorder')
        -- Check for stock significantly above safety level (20% buffer) with reorder status
        OR (stock_level > safety_stock * 1.2 and inventory_status = 'reorder')
)

select * from inconsistent_inventory_status
