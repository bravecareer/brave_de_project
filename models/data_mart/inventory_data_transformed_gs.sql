{{ 
    config(
        materialized='view',
        alias='inventory_data_transformed_gs',
        unique_key='INVENTORY_ID'
    ) 
}}

SELECT 
    INVENTORY_ID::NUMBER(38,0) AS INVENTORY_ID,  
    PRODUCT_ID::VARCHAR AS PRODUCT_ID,  
    WAREHOUSE_ID::NUMBER(38,0) AS WAREHOUSE_ID,
    STOCK_LEVEL::NUMBER(38,0) AS STOCK_LEVEL,
    RESTOCK_DATE::DATE AS RESTOCK_DATE,
    SUPPLIER_ID::NUMBER(38,0) AS SUPPLIER_ID,

    -- Normalize storage condition format
    INITCAP(STORAGE_CONDITION) AS STORAGE_CONDITION,

    -- Normalize inventory status
    CASE 
        WHEN LOWER(INVENTORY_STATUS) IN ('backordered', 'out of stock') THEN 'Backordered'
        WHEN LOWER(INVENTORY_STATUS) = 'in-stock' THEN 'In Stock'
        ELSE 'Unknown'
    END AS NORMALIZED_INVENTORY_STATUS,

    -- Convert LAST_AUDIT_DATE to a readable format
    LAST_AUDIT_DATE::TIMESTAMP_NTZ(9) AS LAST_AUDIT_DATE,

    REORDER_LEVEL::NUMBER(38,0) AS REORDER_LEVEL,
    -- Ensure QUANTITY_IN_STOCK is not less than 0
    GREATEST(QUANTITY_IN_STOCK, 0) AS QUANTITY_IN_STOCK,

    -- Convert RATING to FLOAT with two decimal places
    ROUND(TRY_CAST(REPLACE(RATING, ',', '.') AS FLOAT), 2) AS RATING,

    SALES_VOLUME::NUMBER(38,0) AS SALES_VOLUME,

    -- Convert WEIGHT to FLOAT for calculations
    ROUND(TRY_CAST(REPLACE(WEIGHT, ',', '.') AS FLOAT), 2) AS WEIGHT_KG,

    -- Convert DISCOUNTS to FLOAT for calculations
    ROUND(TRY_CAST(REPLACE(DISCOUNTS, ',', '.') AS FLOAT), 2) AS DISCOUNT_PERCENTAGE,

    SAFETY_STOCK::NUMBER(38,0) AS SAFETY_STOCK,
    AVERAGE_MONTHLY_DEMAND::NUMBER(38,0) AS AVERAGE_MONTHLY_DEMAND,

    -- Convert DATE columns to standard format
    LAST_RESTOCK_DATE::DATE AS LAST_RESTOCK_DATE,
    NEXT_RESTOCK_DATE::DATE AS NEXT_RESTOCK_DATE,

    -- Calculate days since last restock
    DATEDIFF(DAY, LAST_RESTOCK_DATE, CURRENT_DATE) AS DAYS_SINCE_RESTOCK,

    -- Predict next stock depletion date (Avoid division by zero)
    DATEADD(DAY, GREATEST(QUANTITY_IN_STOCK, 0) / NULLIF(AVERAGE_MONTHLY_DEMAND, 0) * 30, CURRENT_DATE) AS ESTIMATED_STOCK_OUT_DATE,

    -- Flag critical stock conditions
    CASE 
        WHEN GREATEST(QUANTITY_IN_STOCK, 0) < REORDER_LEVEL THEN 'Low Stock'
        ELSE 'Sufficient Stock'
    END AS STOCK_STATUS,

    -- Flag urgent replenishment for backordered items
    CASE 
        WHEN LOWER(INVENTORY_STATUS) = 'backordered' AND GREATEST(QUANTITY_IN_STOCK, 0) < REORDER_LEVEL 
        THEN 'Urgent Restock Needed' 
        ELSE 'Normal' 
    END AS RESTOCK_PRIORITY,

    -- Add updated_at column for incremental logic
    CURRENT_TIMESTAMP AS updated_at   -- Assuming the current timestamp for new records

FROM 
    {{ source('de_project', 'inventory_data') }} u

-- Ensure only valid inventory records
WHERE INVENTORY_ID IS NOT NULL
