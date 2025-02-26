/*
    Test: Key Metrics Stability
    Checks for unexplained large fluctuations in key business metrics:
    - DAU: max 25% change day-over-day
    - Conversion rate: max 30% drop day-over-day
    - Average order value: max 20% change day-over-day
*/

WITH 
-- Daily metrics
daily_metrics AS (
    SELECT 
        date_key,
        COUNT(DISTINCT user_id) as dau,
        SUM(conversions) as conversions,
        SUM(impressions) as impressions,
        CASE WHEN SUM(impressions) > 0 
             THEN SUM(conversions)::float / SUM(impressions) 
             ELSE 0 END as conv_rate,
        CASE WHEN SUM(conversions) > 0 
             THEN SUM(revenue)::float / SUM(conversions) 
             ELSE 0 END as aov
    FROM {{ ref('fact_user_behavior_new') }}
    GROUP BY date_key
),

-- Day-over-day changes
changes AS (
    SELECT 
        cur.date_key,
        -- Percent changes
        CASE WHEN prev.dau > 0 
             THEN (cur.dau - prev.dau)::float / prev.dau 
             ELSE 0 END as dau_pct_change,
        CASE WHEN prev.conv_rate > 0 
             THEN (cur.conv_rate - prev.conv_rate)::float / prev.conv_rate 
             ELSE 0 END as conv_rate_pct_change,
        CASE WHEN prev.aov > 0 
             THEN (cur.aov - prev.aov)::float / prev.aov 
             ELSE 0 END as aov_pct_change
    FROM daily_metrics cur
    LEFT JOIN daily_metrics prev 
        ON (cur.date_key - INTERVAL '1 day') = prev.date_key
    WHERE prev.date_key IS NOT NULL
),

-- Flag problematic changes
fluctuations AS (
    SELECT
        date_key,
        CASE
            WHEN ABS(dau_pct_change) > 0.25 
                THEN 'DAU changed by ' || ROUND(dau_pct_change * 100, 1) || '% (limit: 25%)'
            WHEN conv_rate_pct_change < -0.30 
                THEN 'Conv rate dropped by ' || ROUND(ABS(conv_rate_pct_change) * 100, 1) || '% (limit: 30%)'
            WHEN ABS(aov_pct_change) > 0.20 
                THEN 'AOV changed by ' || ROUND(aov_pct_change * 100, 1) || '% (limit: 20%)'
        END as warning
    FROM changes
    WHERE 
        ABS(dau_pct_change) > 0.25 OR
        conv_rate_pct_change < -0.30 OR
        ABS(aov_pct_change) > 0.20
)

SELECT * FROM fluctuations WHERE warning IS NOT NULL
