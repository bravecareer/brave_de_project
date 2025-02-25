{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

-- Get purchase events from user journey
WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.has_purchase,
       uj.search_event_id,
       uj.session_id,
       uj.cart_id,
       uj.event_timestamp as timestamp
   FROM {{ ref('stg_user_journey') }} uj
   WHERE uj.has_purchase = TRUE
      AND event_timestamp >= CURRENT_DATE() - 5
),

-- Calculate transaction metrics
transaction_data AS (
   SELECT
      uj.user_id,
      uj.product_id,
      uj.timestamp,
      uj.search_event_id,
      uj.session_id,
      uj.cart_id,
      uj.has_purchase,      
      COUNT(uj.has_purchase) AS quantity_sold,  -- Count purchase events
      SUM(p.price) AS total_amount  -- Calculate total transaction amount
   FROM user_journey uj
   LEFT JOIN {{ ref('stg_product_data') }} p
     ON uj.product_id = p.product_id
   GROUP BY uj.user_id, uj.product_id, uj.timestamp, uj.search_event_id, uj.session_id, uj.cart_id, uj.has_purchase
)

SELECT * FROM transaction_data
