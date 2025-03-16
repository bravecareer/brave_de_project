{{ config(
   materialized='incremental',
   unique_key=['Inventory_ID', 'Product_ID']
) }}

WITH inventory_data AS (
    SELECT
        -- Inventory data from Inventory_Status
        is.Inventory_ID,
        is.Product_ID,
        is.Warehouse_ID,
        is.Stock_Level,
        is.Quantity_in_stock,
        is.Sales_Volume,
        is.Safety_Stock,
        is.Average_Monthly_Demand,
        is.Last_Restock_Date,
        is.Next_Restock_Date,
        is.Reorder_Level,
        is.Inventory_Status,
        -- Date fields formatted for easy analysis
        TO_DATE(is.Restock_Date, 'YYYY-MM-DD') AS Restock_Date,
        TO_DATE(is.Last_Audit_Date, 'YYYY-MM-DD') AS Last_Audit_Date
    FROM 
        {{ ref('stg_inventory_status_nb') }} is
),

product_data AS (
    SELECT
        -- Product data from Product
        p.Product_ID,
        p.Product_Name,
        p.Product_Category,
        p.Price,
        p.Supplier_ID,
        p.Product_Color,
        p.Rating,
        p.Weight_Grams
    FROM 
        {{ ref('stg_product_nb') }} p
)

SELECT 
    -- Join inventory data with product data
    inv.Inventory_ID,
    inv.Product_ID,
    inv.Warehouse_ID,
    inv.Stock_Level,
    inv.Quantity_in_stock,
    inv.Sales_Volume,
    inv.Safety_Stock,
    inv.Average_Monthly_Demand,
    inv.Last_Restock_Date,
    inv.Next_Restock_Date,
    inv.Reorder_Level,
    inv.Inventory_Status,
    inv.Restock_Date,
    inv.Last_Audit_Date,
    -- Add product-related information
    p.Product_Name,
    p.Product_Category,
    p.Price,
    p.Supplier_ID,
    p.Product_Color,
    p.Rating,
    p.Weight_Grams
FROM 
    inventory_data inv
JOIN 
    product_data p 
    ON inv.Product_ID = p.Product_ID
