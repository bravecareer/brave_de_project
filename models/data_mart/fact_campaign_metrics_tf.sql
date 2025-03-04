{{ config(
    materialized='incremental',
    unique_key=['campaign_id', 'date_key'],
    incremental_strategy='delete+insert'
) }}

-- Dedicated fact table for campaign performance metrics
-- Aggregated by campaign and date to provide easy campaign performance analysis
-- Helps marketing specialists evaluate campaign success and ROI

WITH campaign_events AS (
    -- Get all user events with campaign information
    SELECT
        COALESCE(uj.mkt_campaign, 'Unknown') as campaign_id,
        DATE(uj.event_timestamp) as date_key,
        uj.product_id,
        p.product_category,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        CASE WHEN uj.has_purchase THEN p.price ELSE 0 END AS item_amount,
        -- Calculate funnel stage (1=view, 2=pdp, 3=atc, 4=purchase)
        CASE 
            WHEN uj.has_purchase THEN 4
            WHEN uj.has_atc THEN 3
            WHEN uj.has_pdp THEN 2
            WHEN uj.has_qv THEN 1
            ELSE 0
        END as funnel_stage,
        -- Track impressions and clicks for ad performance analysis
        CASE WHEN uj.mkt_medium = 'cpc' OR uj.mkt_medium = 'display' OR uj.mkt_medium = 'social' THEN 1 ELSE 0 END as ad_impression,
        CASE WHEN (uj.mkt_medium = 'cpc' OR uj.mkt_medium = 'display' OR uj.mkt_medium = 'social') AND (uj.has_qv = TRUE OR uj.has_pdp = TRUE) THEN 1 ELSE 0 END as ad_click
    FROM {{ ref('stg_user_journey_tf') }} uj
    LEFT JOIN {{ ref('stg_product_data_tf') }} p ON uj.product_id = p.product_id
    {% if is_incremental() %}
    WHERE DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
)

-- Aggregate metrics by campaign and date
-- Only include raw metrics, move calculations to BI layer
SELECT
    campaign_id,
    date_key,
    
    -- User engagement counts
    COUNT(*) as total_events,
    COUNT(DISTINCT product_category) as unique_categories_viewed,
    SUM(CASE WHEN funnel_stage >= 1 THEN 1 ELSE 0 END) as total_product_views,
    SUM(CASE WHEN funnel_stage >= 2 THEN 1 ELSE 0 END) as total_product_detail_views,
    SUM(CASE WHEN funnel_stage >= 3 THEN 1 ELSE 0 END) as total_add_to_cart,
    SUM(CASE WHEN funnel_stage >= 4 THEN 1 ELSE 0 END) as total_purchases,
    
    -- Ad performance metrics
    SUM(ad_impression) as impressions,
    SUM(ad_click) as clicks
    
    -- CTR can be calculated in BI layer as clicks/impressions
FROM campaign_events
GROUP BY
    campaign_id,
    date_key
