WITH event_aggregates AS (
    SELECT
        mkt_campaign,
        COUNT(*) AS total_impressions,
        SUM(CASE WHEN has_qv THEN 1 ELSE 0 END) AS total_clicks,
        SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS total_atc_events,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS total_purchases,
        SUM(CASE WHEN has_purchase THEN price ELSE 0 END) AS total_revenue
    FROM {{ ref('stg_user_journey_ba') }}
    WHERE mkt_campaign IS NOT NULL
    GROUP BY mkt_campaign
)

SELECT
    m.mkt_campaign,
    m.mkt_medium,
    m.mkt_source,
    m.mkt_content,
    ea.total_impressions,
    ea.total_clicks,
    ea.total_atc_events,
    ea.total_purchases,
    ea.total_revenue,
    ROUND((ea.total_atc_events * 100.0) / NULLIF(ea.total_clicks, 0), 2) AS atc_conversion_rate,
    ROUND((ea.total_purchases * 100.0) / NULLIF(ea.total_atc_events, 0), 2) AS purchase_conversion_rate,
    ROUND(ea.total_revenue / NULLIF(ea.total_clicks, 0), 2) AS revenue_per_click
FROM event_aggregates ea
LEFT JOIN {{ ref('dim_marketing_search_ba') }} ON m.mkt_campaign = ea.mkt_campaign
