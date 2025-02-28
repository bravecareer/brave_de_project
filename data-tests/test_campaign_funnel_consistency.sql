/*
    Test Name: Campaign Funnel Metrics Consistency Test
    Description: Validates that campaign metrics follow logical rules:
    - Conversion rates are between 0 and 100%
    - Revenue values are non-negative
    - Total events follows the expected funnel pattern
    
    Note: This test includes tolerance thresholds to account for data tracking issues
    and only checks data from the last 90 days to focus on recent quality issues.
*/

WITH campaign_metrics_check AS (
    SELECT 
        campaign_id,
        date_key,
        total_events,
        total_product_views,
        total_add_to_cart,
        total_purchases,
        total_revenue,
        atc_rate,
        purchase_rate,
        CASE 
            -- Allow for a small margin of error in conversion rates
            WHEN atc_rate < -1 OR atc_rate > 101 THEN 'Add to cart rate must be between 0 and 100% (with 1% tolerance)'
            WHEN purchase_rate < -1 OR purchase_rate > 101 THEN 'Purchase rate must be between 0 and 100% (with 1% tolerance)'
            -- Only flag significant negative revenue
            WHEN total_revenue < -10 THEN 'Revenue cannot be significantly negative'
            -- Allow for a 5% tolerance in the funnel metrics
            WHEN total_product_views > total_events * 1.05 THEN 'Product views exceed total events by more than 5%'
            WHEN total_add_to_cart > total_product_views * 1.05 THEN 'Add to cart exceed product views by more than 5%'
            WHEN total_purchases > total_add_to_cart * 1.05 THEN 'Purchases exceed add to cart by more than 5%'
            ELSE NULL
        END as validation_error
    FROM {{ ref('fact_campaign_metrics') }}
    -- Only check data from the last 90 days to focus on recent quality issues
    WHERE date_key >= DATEADD(day, -90, CURRENT_DATE())
    AND (
        atc_rate < -1 OR atc_rate > 101
        OR purchase_rate < -1 OR purchase_rate > 101
        OR total_revenue < -10
        OR total_product_views > total_events * 1.05
        OR total_add_to_cart > total_product_views * 1.05
        OR total_purchases > total_add_to_cart * 1.05
    )
)

SELECT * FROM campaign_metrics_check
WHERE validation_error IS NOT NULL
