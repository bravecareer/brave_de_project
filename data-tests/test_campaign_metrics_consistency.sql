/*
    Test Name: Campaign Metrics Consistency Test
    Description: Validates that campaign metrics follow logical rules:
    - Conversion rates are between 0 and 100%
    - Revenue values are non-negative
    - Total events follows the expected funnel pattern
*/

WITH campaign_metrics_check AS (
    SELECT 
        campaign_id,
        date_key,
        CASE 
            WHEN conversion_rate < 0 OR conversion_rate > 100 THEN 'Conversion rate must be between 0 and 100%'
            WHEN total_revenue < 0 THEN 'Revenue cannot be negative'
            WHEN product_views > total_events THEN 'Product views cannot exceed total events'
            WHEN add_to_cart > product_views THEN 'Add to cart cannot exceed product views'
            WHEN purchases > add_to_cart THEN 'Purchases cannot exceed add to cart'
        END as validation_error
    FROM {{ ref('fact_campaign_metrics') }}
    WHERE conversion_rate < 0 
       OR conversion_rate > 100
       OR total_revenue < 0
       OR product_views > total_events
       OR add_to_cart > product_views
       OR purchases > add_to_cart
)

SELECT * FROM campaign_metrics_check
WHERE validation_error IS NOT NULL
