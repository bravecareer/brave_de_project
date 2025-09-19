{{ config(
   materialized='incremental',
   incremental_strategy='merge',
   unique_key=['user_id','search_event_id','product_id','event_ts']  
) }}

WITH purchases AS (
  SELECT
      uj.user_id,
      uj.product_id,
      uj.search_event_id,
      uj.session_id,
      uj.cart_id,
      uj.event_ts,        -- staged from raw `timestamp`
      uj.collector_ts,    -- staged from raw `collector_tstamp`
      uj.has_purchase
  FROM {{ ref('stg_user_journey_yl') }} uj
  WHERE uj.has_purchase = TRUE
  {% if is_incremental() %}
    -- 2-day sliding window on collection time to capture late arrivals
    AND DATE(uj.collector_ts) >= CURRENT_DATE() - 2
  {% endif %}
),

transaction_data AS (
  SELECT
      p.user_id,
      p.product_id,
      p.search_event_id,
      p.session_id,
      p.cart_id,
      p.event_ts,
      p.collector_ts,
      /* keep original aggregation style */
      COUNT(p.has_purchase) AS quantity_sold,
      SUM(pr.price)         AS total_amount
  FROM purchases p
  LEFT JOIN {{ ref('stg_product_data_yl') }} pr
    ON pr.product_id = p.product_id
  GROUP BY
      p.user_id, p.product_id, p.search_event_id, p.session_id, p.cart_id,
      p.event_ts, p.collector_ts
)

SELECT
  user_id,
  product_id,
  search_event_id,
  session_id,
  cart_id,
  event_ts,
  TO_DATE(event_ts) AS order_date,
  collector_ts,
  quantity_sold,
  total_amount,
  '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at
FROM transaction_data
























 
