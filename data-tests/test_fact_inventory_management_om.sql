-- Test for valid total purchases vs ATC events
SELECT * 
FROM {{ ref('fact_product_performance_om') }} 
WHERE total_purchases > total_atc_events
