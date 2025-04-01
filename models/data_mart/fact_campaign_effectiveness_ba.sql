{{ config(
   materialized='table'
) }}

WITH event_aggregates AS (
    SELECT
        mkt_campaign,
        COUNT(*) AS total_impressions,
        SUM(CASE WHEN has_qv THEN 1 ELSE 0 END) AS total_clicks,
        SUM(CASE WHEN has_atc THEN 1 ELSE 0 END) AS total_atc_events,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) AS total_purchases
    FROM {{ ref('stg_user_journey_ba') }}
    WHERE mkt_campaign IS NOT NULL
    GROUP BY mkt_campaign
),

marketing AS (
    SELECT
        m.mkt_campaign,
        mkt_medium,
        m.mkt_source,
        m.mkt_content
FROM {{ ref('stg_user_journey_ba') }} m
)

SELECT
    ea.mkt_campaign,
    mkt_medium,
    mkt_source,
    mkt_content,
    ea.total_impressions,
    ea.total_clicks,
    ea.total_atc_events,
    ea.total_purchases,
    ROUND((ea.total_atc_events * 100.0) / NULLIF(ea.total_clicks, 0), 2) AS atc_conversion_rate,
    ROUND((ea.total_purchases * 100.0) / NULLIF(ea.total_atc_events, 0), 2) AS purchase_conversion_rate
FROM event_aggregates ea
JOIN marketing m ON m.mkt_campaign = ea.mkt_campaign
