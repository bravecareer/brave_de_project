{{
    config(
        materialized='incremental',
        unique_key=['product_id', 'date_key', 'user_id'],
        incremental_strategy='merge'
    )
}}

-- Filter and prepare purchase data
WITH purchase_events AS (
    SELECT
        uj.product_id,
        uj.user_id,
        uj.event_timestamp,
        p.price
    FROM {{ ref('stg_user_journey_tf') }} uj
    LEFT JOIN {{ ref('stg_product_data_tf') }} p ON uj.product_id = p.product_id
    WHERE uj.has_purchase = TRUE  -- Only include actual purchases
    AND uj.user_id IS NOT NULL
    AND uj.user_id != 'UNKNOWN'
    {% if is_incremental() %}
    AND DATE(uj.event_timestamp) >= CURRENT_DATE() - 5
    {% endif %}
),

-- Aggregate metrics by product, user and date
product_performance AS (
    SELECT
        product_id,
        user_id,
        DATE(event_timestamp) as date_key,
        TRUE as has_purchase,  -- Always true since we filter for purchases
        COUNT(*) as purchase_count,  -- Count all rows in each group
        SUM(price) AS revenue  -- Sum the price directly since all rows are purchases
    FROM purchase_events
    GROUP BY
        product_id,
        user_id,
        date_key
)

-- Final selection
SELECT
    product_id,
    user_id,
    date_key,
    has_purchase,
    purchase_count,
    revenue
FROM product_performance

