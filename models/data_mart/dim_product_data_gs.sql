{{ config(
    materialized='incremental',
    alias='dim_product_data_gs',
    unique_key='product_id'
) }}

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
    quantity_in_stock,
    rating,
    sales_volume,
    weight_grams,
    discount_percentage,
    updated_at
FROM {{ ref('product_data_transformed_gs') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
