WITH campaign_metrics_check AS (
    SELECT 
        campaign_id,
        date_key,
        session_count,
        purchase_count,
        total_revenue,
        impression_count,
        click_count,
        CASE 
            -- Check for negative metrics
            WHEN session_count < 0 THEN 'Session count cannot be negative'
            WHEN purchase_count < 0 THEN 'Purchase count cannot be negative'
            WHEN total_revenue < 0 THEN 'Total revenue cannot be negative'
            WHEN impression_count < 0 THEN 'Impression count cannot be negative'
            WHEN click_count < 0 THEN 'Click count cannot be negative'
            
            -- Allow for a 5% tolerance in the funnel metrics
            WHEN purchase_count > session_count * 1.05 THEN 'Purchases exceed sessions by more than 5%'
            WHEN click_count > impression_count * 1.05 THEN 'Clicks exceed impressions by more than 5%'
            ELSE NULL
        END as validation_error
    FROM {{ ref('fact_campaign_metrics_tf') }}
    -- Only check data from the last 90 days to focus on recent quality issues
    WHERE date_key >= DATEADD(day, -90, CURRENT_DATE())
    AND (
        session_count < 0
        OR purchase_count < 0
        OR total_revenue < 0
        OR impression_count < 0
        OR click_count < 0
        OR purchase_count > session_count * 1.05
        OR click_count > impression_count * 1.05
    )
)

SELECT * FROM campaign_metrics_check
WHERE validation_error IS NOT NULL
