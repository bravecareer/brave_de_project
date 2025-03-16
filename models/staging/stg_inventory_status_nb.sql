{{ config(
    materialized='view'
) }}

WITH raw_inventory_status AS (
    SELECT
        Inventory_ID,
        Product_ID,
        Warehouse_ID,
        Stock_Level,
        Restock_Date,
        Supplier_ID,
        Storage_Condition,
        Inventory_Status,
        Last_Audit_Date,
        Reorder_Level,
        Quantity_in_stock,
        Rating,
        Sales_Volume,
        Weight,
        Discount,
        Safety_Stock,
        Average_Monthly_Demand,
        Last_Restock_Date,
        Next_Restock_Date
    FROM
        {{ source('de_project', 'inventory_data') }} -- refer to the raw table using source() function
)

SELECT 
    Inventory_ID,
    Product_ID,
    Warehouse_ID,
    Stock_Level,
    Restock_Date,
    Supplier_ID,
    Storage_Condition,
    Inventory_Status,
    TO_DATE(SPLIT_PART(Last_Audit_Date, 'T', 1), 'YYYY-MM-DD') AS Last_Audit_Date,
    Reorder_Level,
    Quantity_in_stock,
    ROUND(Rating, 2) AS Rating,
    Sales_Volume,
    ROUND(Weight,2) AS Weight,
    ROUND(Discount,2) AS Discount,
    Safety_Stock,
    Average_Monthly_Demand,
    Last_Restock_Date,
    Next_Restock_Date
FROM raw_inventory_status