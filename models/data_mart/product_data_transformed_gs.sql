{{ config(
    materialized='view',
    alias='product_data_transformed_gs',
    unique_key='PRODUCT_ID'
) }}

SELECT 
    PRODUCT_ID::VARCHAR(50) AS product_id,
    PRODUCT_NAME::VARCHAR(255) AS product_name,
    PRODUCT_CATEGORY::VARCHAR(100) AS product_category,
    PRICE::NUMBER(10,2) AS price,
    SUPPLIER_ID::VARCHAR(50) AS supplier_id,
    PRODUCT_COLOR::VARCHAR(50) AS product_color,
    MANUFACTURING_DATE::DATE AS manufacturing_date,
    EXPIRATION_DATE::DATE AS expiration_date,
    WARRANTY_PERIOD::VARCHAR(50) AS warranty_period,
    QUANTITY_IN_STOCK::NUMBER(10,0) AS quantity_in_stock,
    RATING::NUMBER(3,1) AS rating,
    SALES_VOLUME::NUMBER(10,0) AS sales_volume,
    WEIGHT_GRAMS::NUMBER(10,0) AS weight_grams,
    DISCOUNT_PERCENTAGE::NUMBER(3,0) AS discount_percentage,
    CURRENT_TIMESTAMP AS updated_at -- Track when the record was last transformed
FROM {{ source('de_project', 'product_data') }}
WHERE PRODUCT_ID IS NOT NULL
