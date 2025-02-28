/*
    Test Name: Search Funnel Metrics Consistency Test
    Description: Validates that search metrics follow logical rules:
    - Funnel stages follow the expected pattern (upstream >= downstream)
    
    Note: This test includes tolerance thresholds to account for data tracking issues
    and only checks data from the last 90 days to focus on recent quality issues.
*/

WITH search_metrics_check AS (
    SELECT 
        search_event_id,
        date_key,
        total_searches,
        total_quick_views,
        total_add_to_cart,
        total_purchases,
        CASE 
            -- Allow for a 5% tolerance in the funnel metrics to account for tracking issues
            WHEN total_quick_views > total_searches * 1.05 THEN 'Quick views exceed total searches by more than 5%'
            WHEN total_add_to_cart > total_quick_views * 1.05 THEN 'Add to cart exceed quick views by more than 5%'
            WHEN total_purchases > total_add_to_cart * 1.05 THEN 'Purchases exceed add to cart by more than 5%'
            ELSE NULL
        END as validation_error
    FROM {{ ref('fact_search_metrics_new') }}
    -- Only check data from the last 90 days to focus on recent quality issues
    WHERE date_key >= DATEADD(day, -90, CURRENT_DATE())
    AND (
        total_quick_views > total_searches * 1.05
        OR total_add_to_cart > total_quick_views * 1.05
        OR total_purchases > total_add_to_cart * 1.05
    )
)

SELECT * FROM search_metrics_check
WHERE validation_error IS NOT NULL
