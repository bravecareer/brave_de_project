{{ config(
   materialized='incremental',
   unique_key='product_id'
) }}


WITH product_data AS (
   SELECT
       p.product_id,
       p.supplier_id, --Replacing because noisy/unstructured
      -- i.supplier_id, Removing because multiple suppliers for same product
       p.product_name,
       p.product_category,
       p.price,
       p.product_color,
       p.manufacturing_date,
       p.expiration_date,
       p.warranty_period,
       p.product_rating,
       p.weight_grams,
       p.discount_percentage
   FROM {{ ref( 'stg_products_sp') }} p
   LEFT JOIN {{ ref('stg_inventory_sp') }} i 
   ON p.product_id = i.product_id
)


SELECT * FROM product_data