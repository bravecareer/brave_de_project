{{ config(
   materialized='view',
   unique_key='inventory_id'
) }}

-- Get inventory data with recent audit records
WITH inventory_data AS (
   SELECT
       i.inventory_id,
       i.product_id,
       i.warehouse_id,
       
       -- Stock levels and thresholds (needed by fact_inventory_metrics_new)
       i.stock_level as current_stock_level,
       i.safety_stock as safety_stock_level,
       i.reorder_level as restock_point,
       
       -- Metrics (needed for estimated_days_of_inventory calculation)
       i.average_monthly_demand,
       
       -- Dates
       DATE(CURRENT_TIMESTAMP()) as date_key,
       CURRENT_TIMESTAMP() as last_updated,
       
       -- Add calculated fields
       p.price as unit_price
   FROM {{ ref('stg_inventory_data') }} i
   LEFT JOIN {{ ref('stg_product_data') }} p ON i.product_id = p.product_id
   WHERE i.last_audit_date >= CURRENT_DATE() - 5 
)

SELECT * FROM inventory_data