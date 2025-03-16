{{ config(
    materialized='view'
) }}

WITH source_data AS (
    SELECT
        Product_ID AS product_id,
        Product_name AS product_name,
        Product_category AS product_category,
        Price AS price,
        Supplier_ID AS supplier_id,
        Product_color AS product_color, 
        manufacturing_date,
        expiration_date,
        warranty_period,
        quantity_in_stock,
        Rating AS rating,
        Sales_volume AS sales_volume,
        weight_grams,
        discount_percentage
    FROM {{ source('de_project', 'product_data') }}
),

SELECT * FROM source_data