{{
    config(
        materialized = 'view',
        unique_key = 'inventory_id'
    )

}}

WITH inventory as (

    SELECT 
        i.inventory_id,
        i.product_id,
        i.warehouse_id,
        i.restock_date,
        i.supplier_id,
        i.storage_condition,
        i.inventory_status,
        i.last_audit_date,
        i.reorder_level,
        i.quantity_in_stock,
        i.sales_volume,
        i.safety_stock,
        i.average_monthly_demand,
        i.last_restock_date,
        i.next_restock_date,
        i.rating,
        i.weight,
        i.discounts
    
    FROM
        {{source('de_project', 'inventory_data')}} i
)

SELECT * FROM inventory