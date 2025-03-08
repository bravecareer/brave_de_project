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
    p.PRODUCT_NAME AS PRODUCT_NAME,  -- Might be NULL if no matching product
    u.QUANTITY_IN_STOCK AS QUANTITY_IN_STOCK,  
    u.NEXT_RESTOCK_DATE AS NEXT_RESTOCK_DATE,
    u.updated_at AS UPDATED_AT  -- Pull the updated_at from transformed table

FROM 
    {{ ref('inventory_data_transformed_gs') }} u
LEFT JOIN 
    {{ ref('dim_product_data_gs') }} p
    ON u.PRODUCT_ID = p.PRODUCT_ID  -- LEFT JOIN to avoid dropping unmatched inventory records

-- Ensure only urgent stock requirements
WHERE 
    LOWER(u.INVENTORY_STATUS) = 'backordered' 
    AND u.QUANTITY_IN_STOCK < u.REORDER_LEVEL

{% if is_incremental() %}
-- Use the updated_at column from the target model {{ this }} for incremental filtering
AND u.updated_at > COALESCE(
       (SELECT MAX(updated_at) FROM {{ this }}), 
       '1990-01-01'
)
{% endif %}
