/*
    Test: Conversion Rate Calculation
    Validates conversion rates calculations in user behavior fact table:
    - Rates between 0 and 1
    - Click-through = clicks/impressions
    - Conversion = conversions/clicks
    - Overall conversion = conversions/impressions
*/

WITH conversion_check AS (
    SELECT 
        date_key,
        product_id,
        CASE 
            WHEN click_through_rate < 0 OR click_through_rate > 1
                THEN 'CTR must be between 0-1'
            WHEN conversion_rate < 0 OR conversion_rate > 1
                THEN 'CR must be between 0-1'
            WHEN overall_conversion_rate < 0 OR overall_conversion_rate > 1
                THEN 'OCR must be between 0-1'
            WHEN impressions > 0 AND ABS(click_through_rate - (clicks::float / impressions)) > 0.001
                THEN 'CTR calculation error'
            WHEN clicks > 0 AND ABS(conversion_rate - (conversions::float / clicks)) > 0.001
                THEN 'CR calculation error'
            WHEN impressions > 0 AND ABS(overall_conversion_rate - (conversions::float / impressions)) > 0.001
                THEN 'OCR calculation error'
        END as error
    FROM {{ ref('fact_user_behavior_new') }}
    WHERE impressions > 0
)

SELECT * FROM conversion_check WHERE error IS NOT NULL
