-- tests/test_valid_total_purchases_vs_atc.sql

SELECT *
FROM {{ ref('fact_product_performance_om') }}
WHERE total_purchases > total_atc_events
