{{
    config(
        materialized='incremental',
        alias='dim_inventory_data_gs',
        unique_key='INVENTORY_ID'
    )
}}

SELECT 
    INVENTORY_ID,  
    PRODUCT_ID,  
    WAREHOUSE_ID,
    SUPPLIER_ID,
    STORAGE_CONDITION,  
    INVENTORY_STATUS,
    REORDER_LEVEL,
    SAFETY_STOCK,
    AVERAGE_MONTHLY_DEMAND,
    RATING,
    WEIGHT_KG,
    DISCOUNT_PERCENTAGE,
    SALES_VOLUME,
    STOCK_STATUS,
    RESTOCK_PRIORITY,
    
    -- Already transformed fields from the inventory_data_transformed_gs view
    LAST_RESTOCK_DATE,
    NEXT_RESTOCK_DATE,
    DAYS_SINCE_RESTOCK, 
    ESTIMATED_STOCK_OUT_DATE,

    -- Incremental update tracking
    UPDATED_AT

FROM 
    {{ ref('view_inventory_data_transformed_gs') }}  

{% if is_incremental() %}
-- Only load records that have been updated since the last successful run
WHERE UPDATED_AT > (SELECT COALESCE(MAX(UPDATED_AT), '1900-01-01') FROM {{ this }})
{% endif %}
