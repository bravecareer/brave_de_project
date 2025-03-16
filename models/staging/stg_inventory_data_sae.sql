{{ config(
    materialized='table',
    schema='PROJECT_TEST',
    cluster_by=['warehouse_id', 'restock_date']
) }}

WITH raw_data AS (
    SELECT
        INVENTORY_ID,
        PRODUCT_ID,
        WAREHOUSE_ID,
        STOCK_LEVEL,
        RESTOCK_DATE,
        SUPPLIER_ID,
        STORAGE_CONDITION,
        INVENTORY_STATUS,
        LAST_AUDIT_DATE,
        REORDER_LEVEL,
        QUANTITY_IN_STOCK,
        RATING,
        SALES_VOLUME,
        WEIGHT,
        DISCOUNTS,
        SAFETY_STOCK,
        AVERAGE_MONTHLY_DEMAND,
        LAST_RESTOCK_DATE,
        NEXT_RESTOCK_DATE
    FROM {{ source('de_project', 'inventory_data') }}
    WHERE INVENTORY_ID IS NOT NULL
      AND PRODUCT_ID IS NOT NULL
      AND WAREHOUSE_ID IS NOT NULL
      AND STOCK_LEVEL IS NOT NULL
),

cleaned AS (
    SELECT
        INVENTORY_ID AS inventory_id,
        PRODUCT_ID AS product_id,
        WAREHOUSE_ID AS warehouse_id,
        STOCK_LEVEL AS stock_level,
        RESTOCK_DATE AS restock_date,
        SUPPLIER_ID AS supplier_id,
        INITCAP(TRIM(STORAGE_CONDITION)) AS storage_condition,
        INITCAP(TRIM(INVENTORY_STATUS)) AS inventory_status,
        LAST_AUDIT_DATE AS last_audit_date,
        REORDER_LEVEL AS reorder_level,
        QUANTITY_IN_STOCK AS quantity_in_stock,
        -- Convert string decimals: replace commas with periods and round to 2 decimal places
        ROUND(TRY_CAST(REPLACE(RATING, ',', '.') AS FLOAT), 2) AS rating,
        SALES_VOLUME AS sales_volume,
        ROUND(TRY_CAST(REPLACE(WEIGHT, ',', '.') AS FLOAT), 2) AS weight,
        ROUND(TRY_CAST(REPLACE(DISCOUNTS, ',', '.') AS FLOAT), 2) AS discounts,
        SAFETY_STOCK AS safety_stock,
        AVERAGE_MONTHLY_DEMAND AS average_monthly_demand,
        LAST_RESTOCK_DATE AS last_restock_date,
        NEXT_RESTOCK_DATE AS next_restock_date,
        -- Calculated field: difference between stock and reorder level
        STOCK_LEVEL - REORDER_LEVEL AS stock_vs_reorder,
        CURRENT_TIMESTAMP() AS load_timestamp,
        -- Flag conversion errors for numeric fields that were originally strings
        CASE 
            WHEN (RATING IS NOT NULL AND ROUND(TRY_CAST(REPLACE(RATING, ',', '.') AS FLOAT), 2) IS NULL)
              OR (WEIGHT IS NOT NULL AND ROUND(TRY_CAST(REPLACE(WEIGHT, ',', '.') AS FLOAT), 2) IS NULL)
              OR (DISCOUNTS IS NOT NULL AND ROUND(TRY_CAST(REPLACE(DISCOUNTS, ',', '.') AS FLOAT), 2) IS NULL)
            THEN 1
            ELSE 0
        END AS conversion_error_flag
    FROM raw_data
    WHERE STOCK_LEVEL >= 0
      AND QUANTITY_IN_STOCK >= 0
      AND NEXT_RESTOCK_DATE IS NOT NULL
      -- Ensure date consistency; adjust this if it's too strict given backdated data:
      AND LAST_RESTOCK_DATE < RESTOCK_DATE
      AND RESTOCK_DATE < NEXT_RESTOCK_DATE
),

deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY inventory_id ORDER BY load_timestamp DESC) AS rn
    FROM cleaned
)

SELECT 
    inventory_id,
    product_id,
    warehouse_id,
    stock_level,
    restock_date,
    supplier_id,
    storage_condition,
    inventory_status,
    last_audit_date,
    reorder_level,
    quantity_in_stock,
    rating,
    sales_volume,
    weight,
    discounts,
    safety_stock,
    average_monthly_demand,
    last_restock_date,
    next_restock_date,
    stock_vs_reorder,
    conversion_error_flag,
    load_timestamp
FROM deduped
WHERE rn = 1
