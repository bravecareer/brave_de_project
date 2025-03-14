-- Test for invalid views in product_performance
WITH invalid_views AS (
  SELECT
    product_id,
    views
  FROM {{ ref('FACT_PRODUCT_PERFORMANCE_JC') }}
  WHERE views < 0
)
SELECT * FROM invalid_views