/*
    Test Name: Stock Level Consistency Test
    Description: Validates that stock levels are within valid ranges:
    - Stock level should not be negative
    - Stock level should not exceed maximum capacity
    - Stock level should be above safety stock when not in reorder status
*/

with invalid_stock_levels as (
    select 
        inventory_id,
        product_id,
        warehouse_id,
        stock_level,
        max_capacity,
        safety_stock,
        inventory_status
    from {{ ref('stg_inventory_data') }}
    where 
        -- Check for negative stock
        stock_level < 0 
        -- Check for exceeding maximum capacity
        or stock_level > max_capacity
        -- Check for stock below safety level without reorder status
        or (stock_level < safety_stock and inventory_status != 'reorder')
)

select * from invalid_stock_levels
