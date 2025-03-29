-- models/marts/inventory_summary.sql

WITH inventory_aggregates AS (
    SELECT
        i.product_id,
        i.warehouse_id,
        SUM(i.quantity_in_stock) AS total_stock_level,
        SUM(i.safety_stock) AS total_safety_stock,
        SUM(i.reorder_level) AS total_reorder_level,
        SUM(i.average_monthly_demand) AS total_monthly_demand,
        MAX(i.next_restock_date) AS next_restock_date,
        MAX(i.last_restock_date) AS last_restock_date,
        COUNT(DISTINCT i.inventory_id) AS inventory_items
    FROM {{ ref('stg_inventory_data_ba') }} i 
    GROUP BY i.product_id, i.warehouse_id
),

-- Join with product metadata
product_enriched AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_category,
        p.supplier_id,
        p.price,
        p.weight_grams
    FROM {{ ref('stg_product_data_ba') }} p
),

-- Join with warehouse metadata
warehouse_enriched AS (
    SELECT DISTINCT
        i.warehouse_id,
        i.storage_condition,
        i.inventory_status
    FROM {{ ref('stg_inventory_data_ba') }} i
)

SELECT
    ia.product_id,
    pe.product_name,
    pe.product_category,
    pe.supplier_id,
    pe.price,
    pe.weight_grams,
    ia.warehouse_id,
    we.storage_condition,
    we.inventory_status,
    ia.total_stock_level,
    ia.total_safety_stock,
    ia.total_reorder_level,
    ia.total_monthly_demand,
    ia.inventory_items,
    ia.next_restock_date,
    ia.last_restock_date,
    CASE
        WHEN ia.total_stock_level <= ia.total_safety_stock THEN 'Reorder Required'
        WHEN ia.total_stock_level <= ia.total_reorder_level THEN 'Restock Soon'
        ELSE 'Sufficient Stock'
    END AS stock_status
FROM inventory_aggregates ia
LEFT JOIN product_enriched pe ON ia.product_id = pe.product_id
LEFT JOIN warehouse_enriched we ON ia.warehouse_id = we.warehouse_id
