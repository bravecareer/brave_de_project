-- Inventory data refreshed as a view, unique on inventory_id
{{ config(
    materialized='view',
    unique_key='inventory_id'
) }}

-- Define cutoff date (records audited within the last 5 days)
WITH date_threshold AS (
    SELECT CURRENT_DATE() - INTERVAL '5 DAYS' AS audit_cutoff
),

-- Select recent inventory data based on audit date
inventory_data AS (
    SELECT
        i.inventory_id,                -- Inventory unique identifier
        i.reorder_level,               -- Trigger point for reordering
        i.product_id,                  -- Product identifier
        i.quantity_in_stock,           -- Current stock quantity
        i.warehouse_id,                -- Warehouse storage identifier
        i.rating,                      -- Inventory condition rating
        i.stock_level,                 -- Stock level status indicator
        i.sales_volume,                -- Sales volume metric
        i.restock_date,                -- Scheduled restock date
        i.weight,                      -- Item weight
        i.supplier_id,                 -- Supplier identifier
        i.discounts,                   -- Current discounts applied
        i.storage_condition,           -- Required storage conditions
        i.safety_stock,                -- Safety stock level
        i.inventory_status,            -- Current inventory status
        i.average_monthly_demand,      -- Calculated monthly demand average
        i.last_audit_date,             -- Last audit timestamp
        i.last_restock_date,           -- Last restock timestamp
        i.next_restock_date            -- Planned next restock timestamp
    FROM {{ source('de_project', 'inventory_data') }} i
    WHERE i.last_audit_date >= (SELECT audit_cutoff FROM date_threshold)
)

-- Final selection of cleaned inventory data for downstream use
SELECT * FROM inventory_data
