{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

-- Simplified product performance analysis table
-- Aggregates key metrics: views, ATC events, and purchases
WITH product_events AS (
    -- Get product-related user events
    SELECT
        uj.product_id,
        DATE(uj.event_timestamp) as date_key,
        p.product_category,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase
    FROM {{ ref('stg_user_journey') }} uj
    LEFT JOIN {{ ref('stg_product_data') }} p ON uj.product_id = p.product_id
    {% if is_incremental() %}
    WHERE DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Aggregate metrics by product and date
product_metrics AS (
    SELECT
        product_id,
        date_key,
        MAX(product_category) as product_category,
        
        -- Key metrics: views, ATC events, and purchases
        SUM(CASE WHEN has_qv = TRUE OR has_pdp = TRUE THEN 1 ELSE 0 END) as total_views,
        SUM(CASE WHEN has_atc = TRUE THEN 1 ELSE 0 END) as total_atc,
        SUM(CASE WHEN has_purchase = TRUE THEN 1 ELSE 0 END) as total_purchases
    FROM product_events
    GROUP BY product_id, date_key
)

SELECT * FROM product_metrics
