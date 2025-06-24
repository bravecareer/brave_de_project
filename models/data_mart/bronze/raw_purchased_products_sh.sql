--- Get raw data of Product ids from the data source----
SELECT
product_id,
product_name,
product_category,
price,
product_color,
manufacturing_date,
expiration_date,
warranty_period,
rating,
weight_grams,
discount_percentage 
FROM {{source('de_project', 'product_data')}}
WHERE manufacturing_date >= CURRENT_DATE() - 5
--where uj.has_purchase = TRUE
--{% if is_incremental() %}
  --AND uj.updated_at > COALESCE((SELECT MAX(updated_at) FROM {{ this }}), '1990-01-01')
--{% endif %}         
