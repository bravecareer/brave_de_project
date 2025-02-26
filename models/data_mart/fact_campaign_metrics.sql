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
        DATE(uj.timestamp) as date_key,
        uj.product_category,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        uj.item_amount,
        uj.funnel_stage
    FROM {{ ref('fact_user_behavior_new') }} uj
    {% if is_incremental() %}
    WHERE DATE(uj.timestamp) >= CURRENT_DATE() - 5
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
        
        -- Funnel metrics
        SUM(CASE WHEN has_qv = TRUE THEN 1 ELSE 0 END) as total_product_views,
        SUM(CASE WHEN has_pdp = TRUE THEN 1 ELSE 0 END) as total_product_detail_views,
        SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) as total_add_to_cart,
        SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) as total_purchases,
        
        -- Revenue metrics
        SUM(item_amount) as total_revenue,
        
        -- Calculate conversion rates
        ROUND(SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN has_qv = TRUE THEN 1 ELSE 0 END), 0), 2) as atc_rate,
        
        ROUND(SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END), 0), 2) as purchase_rate,
        
        -- Calculate average funnel stage reached (higher is better)
        ROUND(AVG(funnel_stage), 2) as avg_funnel_depth,
        
        -- Revenue per event (campaign efficiency)
        ROUND(SUM(item_amount) / NULLIF(COUNT(*), 0), 2) as revenue_per_event,
        
        -- Return on investment placeholder (needs campaign cost data)
        -- This would typically be: (total_revenue - campaign_cost) / campaign_cost
        -- For now, we'll just use revenue as a proxy
        SUM(item_amount) as campaign_value
    FROM campaign_events
    GROUP BY
        campaign_id,
        source,
        medium,
        date_key
)

SELECT * FROM daily_campaign_metrics
