{{ config(
    materialized='incremental',
    unique_key=['campaign_id', 'date_key'],
    incremental_strategy='merge'
) }}

-- Dedicated fact table for campaign performance metrics
-- Aggregates metrics by campaign and date

WITH campaign_events AS (
    SELECT
        uj.mkt_campaign as campaign_id,
        DATE(uj.event_timestamp) as date_key,
        uj.session_id,
        uj.has_purchase,
        CASE WHEN uj.has_purchase THEN p.price ELSE 0 END AS item_amount,
        -- Track impressions and clicks for ad performance analysis
        CASE WHEN uj.mkt_medium = 'cpc' OR uj.mkt_medium = 'display' OR uj.mkt_medium = 'social' THEN 1 ELSE 0 END as is_ad_impression,
        CASE WHEN (uj.mkt_medium = 'cpc' OR uj.mkt_medium = 'display' OR uj.mkt_medium = 'social') AND (uj.has_qv = TRUE OR uj.has_pdp = TRUE) THEN 1 ELSE 0 END as is_ad_click
    FROM {{ ref('stg_user_journey_tf') }} uj
    LEFT JOIN {{ ref('stg_product_data_tf') }} p ON uj.product_id = p.product_id
    WHERE uj.mkt_campaign IS NOT NULL 
    AND uj.mkt_campaign != 'UNKNOWN'
    {% if is_incremental() %}
    AND DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
)

SELECT
    campaign_id,
    date_key,
    COUNT(session_id) as session_count,
    SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) as purchase_count,
    SUM(item_amount) as total_revenue,
    SUM(is_ad_impression) as impression_count,
    SUM(is_ad_click) as click_count
FROM campaign_events
GROUP BY
    campaign_id,
    date_key



