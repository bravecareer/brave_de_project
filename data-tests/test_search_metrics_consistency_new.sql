-- Validate key logical relationships in search metrics
-- Ensure funnel metrics follow the correct sequence: Searches >= Views >= Add to Cart >= Purchases

WITH metrics_check AS (
    SELECT 
        search_event_id,
        date_key,
        CASE 
            WHEN total_quick_views > total_searches THEN 'Quick views cannot exceed total searches'
            WHEN total_add_to_cart > total_quick_views THEN 'Add to cart cannot exceed quick views'
            WHEN total_purchases > total_add_to_cart THEN 'Purchases cannot exceed add to cart'
        END as validation_error
    FROM {{ ref('fact_search_metrics_new') }}  
    WHERE total_quick_views > total_searches
        OR total_add_to_cart > total_quick_views
        OR total_purchases > total_add_to_cart
)

SELECT * FROM metrics_check
WHERE validation_error IS NOT NULL
