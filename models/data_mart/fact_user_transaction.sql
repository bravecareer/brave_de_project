{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.has_purchase,
       uj.search_event_id,
       uj.session_id,
       uj.cart_id,
       uj.timestamp
   FROM {{ source('de_project', 'user_journey') }} uj
   WHERE uj.has_purchase = TRUE
),

transaction_data AS (
   SELECT
      uj.user_id,
      uj.product_id,
      uj.timestamp,
      uj.search_event_id,
      uj.session_id,
      uj.cart_id,
      uj.has_purchase,      
      COUNT(uj.has_purchase) AS quantity_sold, -- Counting the number of purchase events
      SUM(p.price) AS total_amount -- Summing the price of the purchased products
   FROM user_journey uj
   LEFT JOIN {{ source('de_project', 'product_data') }} p
     ON uj.product_id = p.product_id
   GROUP BY uj.user_id, uj.product_id, uj.timestamp, uj.search_event_id, uj.session_id, uj.cart_id, uj.has_purchase
)

SELECT * FROM transaction_data
