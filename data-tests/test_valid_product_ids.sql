-- Test valid product_ids
SELECT product_id
FROM {{ ref('fact_product_performance_PS') }} AS fc
WHERE NOT EXISTS (
  SELECT 1
  FROM {{ ref('stg_product_data_PS') }} AS spd
  WHERE spd.product_id = fc.product_id
)
