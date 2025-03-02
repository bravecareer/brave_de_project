{{ config(
    materialized='incremental',
    unique_key=['product_id', 'date_key'],
    incremental_strategy='delete+insert'
) }}

-- Fact table for product performance analysis
-- Aggregates key metrics by product and date
-- Helps product managers analyze product performance with views, add-to-cart, and purchase metrics

WITH product_events AS (
    -- Get all user events with product information
    SELECT
        uj.product_id,
        DATE(uj.timestamp) as date_key,
        p.product_category,
        p.price as unit_price,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        CASE WHEN uj.has_purchase = TRUE THEN p.price ELSE 0 END AS item_amount
    FROM {{ ref('stg_user_journey') }} uj
    LEFT JOIN {{ ref('stg_product_data') }} p ON uj.product_id = p.product_id
    {% if is_incremental() %}
    WHERE DATE(uj.timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Aggregate metrics by product and date
daily_product_metrics AS (
    SELECT
        product_id,
        date_key,
        MAX(product_category) as product_category,
        MAX(unit_price) as unit_price,
        
        -- Key metrics from screenshot: views, ATC events, and purchases
        SUM(CASE WHEN has_qv = TRUE OR has_pdp = TRUE THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) as total_atc,
        SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) as total_purchases,
        
        -- Revenue metric (important for business)
        SUM(item_amount) as total_revenue,
        
        -- One conversion rate that's most relevant
        ROUND(SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) * 100.0 / 
              NULLIF(SUM(CASE WHEN has_qv = TRUE OR has_pdp = TRUE THEN 1 ELSE 0 END), 0), 2) as view_to_atc_rate
    FROM product_events
    GROUP BY
        product_id,
        date_key
)

SELECT * FROM daily_product_metrics
