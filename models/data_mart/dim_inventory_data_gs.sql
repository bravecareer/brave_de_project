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
    NORMALIZED_INVENTORY_STATUS,
    REORDER_LEVEL,
    SAFETY_STOCK,
    AVERAGE_MONTHLY_DEMAND,
    RATING,
    WEIGHT_KG,
    DISCOUNT_PERCENTAGE,
    SALES_VOLUME,
    STOCK_STATUS,
    RESTOCK_PRIORITY,
    UPDATED_AT
FROM 
    {{ ref('inventory_data_transformed_gs') }}  

{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
