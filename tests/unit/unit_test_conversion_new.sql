-- Unit test for search conversion rate calculations
-- Tests if quick_view_rate and atc_rate are calculated correctly

WITH test_data AS (
    SELECT
        search_event_id,
        date_key,
        total_searches,
        total_quick_views,
        total_add_to_cart,
        quick_view_rate,
        atc_rate,
        -- Expected rates
        ROUND(CASE 
            WHEN total_searches = 0 THEN 0 
            ELSE total_quick_views * 100.0 / total_searches 
        END, 2) as expected_quick_view_rate,
        ROUND(CASE 
            WHEN total_quick_views = 0 THEN 0 
            ELSE total_add_to_cart * 100.0 / total_quick_views 
        END, 2) as expected_atc_rate
    FROM {{ ref('fact_search_metrics_new') }}
),

validation_errors AS (
    SELECT 
        search_event_id,
        date_key,
        'Quick view rate error' as error_type,
        quick_view_rate as actual_rate,
        expected_quick_view_rate as expected_rate
    FROM test_data
    WHERE ABS(quick_view_rate - expected_quick_view_rate) > 0.01

    UNION ALL

    SELECT 
        search_event_id,
        date_key,
        'ATC rate error' as error_type,
        atc_rate as actual_rate,
        expected_atc_rate as expected_rate
    FROM test_data
    WHERE ABS(atc_rate - expected_atc_rate) > 0.01
)

SELECT * FROM validation_errors
