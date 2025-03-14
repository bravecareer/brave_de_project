{{ config(
    materialized='table',
    unique_key='product_id'
) }}

WITH cleaned_product_data AS (
    SELECT
        p.product_id,
        p.product_name,
        p.product_category,
        p.price,
        p.supplier_id,
        p.product_color,
        p.manufacturing_date,
        p.expiration_date,
        p.warranty_period,
        p.weight_grams,
        p.discount_percentage
    FROM {{ source('de_project', 'product_data') }} p
)

SELECT
    product_id,
    product_name,
    product_category,
    price,
    supplier_id,
    product_color,
    manufacturing_date,
    expiration_date,
    warranty_period,
    weight_grams,
    discount_percentage
FROM cleaned_product_data