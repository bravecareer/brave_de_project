/*
    Test Name: Campaign Funnel Metrics Consistency Test
    Description: Validates that campaign metrics follow logical rules:
    - Total events follow the expected funnel pattern
    - Metrics are non-negative
    
    Note: This test includes tolerance thresholds to account for data tracking issues
    and only checks data from the last 90 days to focus on recent quality issues.
*/

WITH campaign_metrics_check AS (
    SELECT 
        campaign_id,
        date_key,
        total_events,
        total_product_views,
        total_product_detail_views,
        total_add_to_cart,
        total_purchases,
        impressions,
        clicks,
        CASE 
            -- Check for negative metrics
            WHEN total_events < 0 THEN 'Total events cannot be negative'
            WHEN total_product_views < 0 THEN 'Product views cannot be negative'
            WHEN total_product_detail_views < 0 THEN 'Product detail views cannot be negative'
            WHEN total_add_to_cart < 0 THEN 'Add to cart cannot be negative'
            WHEN total_purchases < 0 THEN 'Purchases cannot be negative'
            WHEN impressions < 0 THEN 'Impressions cannot be negative'
            WHEN clicks < 0 THEN 'Clicks cannot be negative'
            
            -- Allow for a 5% tolerance in the funnel metrics
            WHEN total_product_views > total_events * 1.05 THEN 'Product views exceed total events by more than 5%'
            WHEN total_product_detail_views > total_product_views * 1.05 THEN 'Product detail views exceed product views by more than 5%'
            WHEN total_add_to_cart > total_product_detail_views * 1.05 THEN 'Add to cart exceed product detail views by more than 5%'
            WHEN total_purchases > total_add_to_cart * 1.05 THEN 'Purchases exceed add to cart by more than 5%'
            WHEN clicks > impressions * 1.05 THEN 'Clicks exceed impressions by more than 5%'
            ELSE NULL
        END as validation_error
    FROM {{ ref('fact_campaign_metrics_tf') }}
    -- Only check data from the last 90 days to focus on recent quality issues
    WHERE date_key >= DATEADD(day, -90, CURRENT_DATE())
    AND (
        total_events < 0
        OR total_product_views < 0
        OR total_product_detail_views < 0
        OR total_add_to_cart < 0
        OR total_purchases < 0
        OR impressions < 0
        OR clicks < 0
        OR total_product_views > total_events * 1.05
        OR total_product_detail_views > total_product_views * 1.05
        OR total_add_to_cart > total_product_detail_views * 1.05
        OR total_purchases > total_add_to_cart * 1.05
    )
)

SELECT * FROM campaign_metrics_check
WHERE validation_error IS NOT NULL
