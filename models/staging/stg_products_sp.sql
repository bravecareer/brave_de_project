{{ config(
   materialized='view',
   unique_key='product_id'
) }}




WITH products AS (

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
        p.quantity_in_stock,
        p.rating AS product_rating,  -- Renaming for clarity
        p.sales_volume,
        p.weight_grams,
        p.discount_percentage

    FROM {{ source('de_project', 'product_data') }} p
    
)

SELECT * FROM products