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
        COALESCE(uj.mkt_source, 'Unknown') as source,
        COALESCE(uj.mkt_medium, 'Unknown') as medium,
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
        END as funnel_stage
    FROM {{ ref('stg_user_journey') }} uj
    LEFT JOIN {{ ref('stg_product_data') }} p ON uj.product_id = p.product_id
    {% if is_incremental() %}
    WHERE DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Aggregate metrics by campaign, source, medium and date
daily_campaign_metrics AS (
    SELECT
        campaign_id,
        source,
        medium,
        date_key,
        
        -- User engagement counts
        COUNT(*) as total_events,
        COUNT(DISTINCT product_category) as unique_categories_viewed,
        SUM(CASE WHEN funnel_stage >= 1 THEN 1 ELSE 0 END) as total_product_views,
        SUM(CASE WHEN funnel_stage >= 2 THEN 1 ELSE 0 END) as total_product_detail_views,
        SUM(CASE WHEN funnel_stage >= 3 THEN 1 ELSE 0 END) as total_add_to_cart,
        SUM(CASE WHEN funnel_stage >= 4 THEN 1 ELSE 0 END) as total_purchases,
        
        -- Revenue metric
        SUM(item_amount) as total_revenue,
        
        -- Calculate conversion rates
        ROUND(
            SUM(CASE WHEN funnel_stage >= 3 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN funnel_stage >= 1 THEN 1 ELSE 0 END), 0), 
            2
        ) as atc_rate,
        
        ROUND(
            SUM(CASE WHEN funnel_stage >= 4 THEN 1 ELSE 0 END) * 100.0 / 
            NULLIF(SUM(CASE WHEN funnel_stage >= 1 THEN 1 ELSE 0 END), 0), 
            2
        ) as purchase_rate,
        
        -- Calculate average funnel depth
        ROUND(AVG(funnel_stage), 2) as avg_funnel_depth,
        
        -- Calculate revenue per event
        ROUND(
            SUM(item_amount) / NULLIF(COUNT(*), 0), 
            2
        ) as revenue_per_event,
        
        -- Calculate campaign value (proxy for ROI)
        ROUND(
            SUM(item_amount) * 
            (SUM(CASE WHEN funnel_stage >= 4 THEN 1 ELSE 0 END) * 1.0 / 
             NULLIF(COUNT(*), 0)), 
            2
        ) as campaign_value
    FROM campaign_events
    GROUP BY
        campaign_id,
        source,
        medium,
        date_key
)

SELECT * FROM daily_campaign_metrics
