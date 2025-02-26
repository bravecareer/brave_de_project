{{ config(
    materialized='incremental',
    unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp'],
    incremental_strategy='delete+insert'
) }}

-- Combined user behavior fact table that integrates both user engagement and transactions
-- This table replaces the previous fact_user_engagement and fact_user_transaction tables

-- Get all user journey events
WITH user_journey AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.search_event_id,
        uj.cart_id,
        uj.session_id,
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        uj.event_timestamp as timestamp,
        uj.mkt_campaign,
        uj.mkt_source,
        uj.mkt_medium
    FROM {{ ref('stg_user_journey') }} uj
    {% if is_incremental() %}
    WHERE uj.event_timestamp >= CURRENT_DATE() - 5
    {% endif %}
),

-- Filter for active users only
valid_users AS (
    SELECT
        u.user_id
    FROM {{ ref('stg_user_data') }} u
    WHERE u.account_status = 'active'
),

-- Get product details including price
product_data AS (
    SELECT
        p.product_id,
        p.price,
        p.product_category
    FROM {{ ref('stg_product_data') }} p
),

-- Calculate behavior metrics including purchases when applicable
user_behavior AS (
    SELECT
        uj.user_id,
        uj.product_id,
        uj.search_event_id,
        uj.session_id,
        uj.cart_id,
        uj.timestamp,
        uj.mkt_campaign,
        uj.mkt_source,
        uj.mkt_medium,
        
        -- Engagement flags
        uj.has_qv,
        uj.has_pdp,
        uj.has_atc,
        uj.has_purchase,
        
        -- Transaction metrics (only populated for purchase events)
        CASE WHEN uj.has_purchase = TRUE THEN 1 ELSE 0 END AS quantity_sold,
        CASE WHEN uj.has_purchase = TRUE THEN pd.price ELSE 0 END AS item_amount,
        
        -- Additional product context
        pd.product_category,
        
        -- Add calculated fields
        CASE
            WHEN uj.has_purchase THEN 'Purchase'
            WHEN uj.has_atc THEN 'Add to Cart'
            WHEN uj.has_pdp THEN 'Product Detail View'
            WHEN uj.has_qv THEN 'Quick View'
            ELSE 'Browse'
        END AS event_type,
        
        -- Track funnel progression
        CASE
            WHEN uj.has_purchase THEN 4
            WHEN uj.has_atc THEN 3
            WHEN uj.has_pdp THEN 2
            WHEN uj.has_qv THEN 1
            ELSE 0
        END AS funnel_stage
    FROM user_journey uj
    JOIN valid_users vu ON uj.user_id = vu.user_id
    LEFT JOIN product_data pd ON uj.product_id = pd.product_id
)

SELECT * FROM user_behavior
