/*InventoryItemKey	Surrogate key
InventoryID, ProductID, WarehouseID	
StockLevel, RestockDate, StorageCondition	
InventoryStatus, LastAuditDate, ReorderLevel	
QuantityInStock, SafetyStock, SalesVolume	
AverageMonthlyDemand, LastRestockDate, NextRestockDate
*/
{{ config(
   materialized='table',
   unique_key='inventory_id'
) }}

WITH inventory AS (
    SELECT
        i.inventory_id,
        i.product_id,
        i.warehouse_id,
        i.stock_level,
        i.restock_date,
        i.supplier_id,
        i.storage_condition,
        i.inventory_status,
        i.last_audit_date,
        i.reorder_level,
        i.quantity_in_stock,
        i.rating,
        i.sales_volume,
        i.weight,
        i.discounts,
        i.safety_stock,
        i.average_monthly_demand,
        i.last_restock_date,
        i.next_restock_date
    FROM {{ ref('stg_inventory_data_ba') }} i
)

SELECT * FROM inventory