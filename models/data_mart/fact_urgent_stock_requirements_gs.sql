{{ 
    config(
        materialized='incremental',
        alias='fact_urgent_inventory_requirements_gs',
        unique_key='inventory_id'
    ) 
}}

SELECT 
    u.INVENTORY_ID AS INVENTORY_ID,  
    u.PRODUCT_ID AS PRODUCT_ID,  
    p.PRODUCT_NAME AS PRODUCT_NAME,  
    u.QUANTITY_IN_STOCK AS QUANTITY_IN_STOCK,  
    u.NEXT_RESTOCK_DATE AS NEXT_RESTOCK_DATE,
    CURRENT_TIMESTAMP AS UPDATED_AT  -- Use CURRENT_TIMESTAMP here instead

FROM 
    {{ ref('inventory_data_transformed_gs') }} u
JOIN 
    {{ ref('dim_product_data_gs') }} p
    ON u.PRODUCT_ID = p.PRODUCT_ID

-- Ensure only urgent stock requirements
WHERE 
    LOWER(u.NORMALIZED_INVENTORY_STATUS) = 'backordered' 
    AND u.QUANTITY_IN_STOCK < u.REORDER_LEVEL

{% if is_incremental() %}
-- Explicitly reference the updated_at column from the target model {{ this }} to avoid ambiguity
AND u.updated_at > COALESCE(
       (SELECT MAX({{ this }}.updated_at) FROM {{ this }}), 
       '1990-01-01')
{% endif %}
