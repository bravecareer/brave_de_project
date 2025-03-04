{{ config(
    materialized='incremental',
    unique_key=['product_id', 'date_key', 'campaign_id']
) }}

-- Simplified product performance analysis table
-- Aggregates key metrics: views, ATC events, and purchases
WITH product_events AS (
    -- Get product-related user events
    SELECT
        uj.product_id,
        DATE(uj.event_timestamp) as date_key,
        -- Add campaign_id to track product performance by marketing campaign
        COALESCE(uj.mkt_campaign, 'Unknown') as campaign_id,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        CASE WHEN uj.has_purchase THEN p.price ELSE 0 END AS item_amount
    FROM {{ ref('stg_user_journey_tf') }} uj
    LEFT JOIN {{ ref('stg_product_data_tf') }} p ON uj.product_id = p.product_id
    {% if is_incremental() %}
    WHERE DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Aggregate metrics by product, campaign and date
product_metrics AS (
    SELECT
        product_id,
        date_key,
        campaign_id,
        -- Removed product_category as it can be referenced from dim_product_tf
        
        -- Key metrics: views, ATC events, and purchases
        SUM(CASE WHEN has_qv = TRUE OR has_pdp = TRUE THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) as total_atc,
        SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) as total_purchases,
        SUM(item_amount) as total_revenue
    FROM product_events
    GROUP BY product_id, date_key, campaign_id
)

SELECT * FROM product_metrics
