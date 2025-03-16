{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

WITH staged_product AS (
    SELECT *
    FROM {{ ref('stg_product_nb') }}
),

final_dim_product AS (
    SELECT
        product_id,
        product_name,
        product_category,
        price,
        supplier_id,
        COALESCE(product_color, 'Unknown') AS product_color,  -- Fix missing colors
        manufacturing_date,
        expiration_date,
        warranty_period,
        quantity_in_stock,
        rating,
        sales_volume,
        weight_grams,
        discount_percentage
    FROM staged_product
    WHERE quantity_in_stock >= 0  -- Ensuring logical correctness
)

SELECT * FROM final_dim_product
