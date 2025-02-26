/*
    Test Name: Stock Level Consistency Test
    Description: Validates inventory status is consistent with stock levels relative to safety stock
    - Stock level should be above safety stock when not in reorder status
    - Stock level should be below safety stock when in reorder status
*/

with inconsistent_inventory_status as (
    select 
        inventory_id,
        product_id,
        warehouse_id,
        stock_level,
        safety_stock,
        inventory_status
    from {{ ref('stg_inventory_data') }}
    where 
        -- Check for stock below safety level without reorder status
        (stock_level < safety_stock and inventory_status != 'reorder')
        -- Check for stock above safety level with reorder status
        OR (stock_level > safety_stock and inventory_status = 'reorder')
)

select * from inconsistent_inventory_status
