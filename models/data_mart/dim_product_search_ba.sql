{{ config(
   materialized='table',
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
        p.rating,
        p.sales_volume,
        p.weight_grams,
        p.discount_percentage
FROM {{ ref('stg_product_data_ba') }} p
)

SELECT * FROM products