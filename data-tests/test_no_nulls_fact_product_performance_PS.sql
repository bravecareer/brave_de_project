-- Test for no NULL values in critical columns for fact_product_performance_PS
SELECT
  product_id
FROM {{ ref('fact_product_performance_PS') }}
WHERE product_id IS NULL
  OR total_qv_events IS NULL
  OR total_pdp_views IS NULL
  OR total_atc_events IS NULL
  OR quantity_sold IS NULL
  OR total_sales_amount IS NULL
