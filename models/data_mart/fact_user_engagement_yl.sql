{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='journey_event_sk'
) }}

WITH engagement_base AS (
  SELECT
      -- PK / grain from staging
      journey_event_sk,

      -- naturals kept for analytics & tests
      user_id,
      product_id,
      search_event_id,
      session_id,
      cart_id,

      -- normalized booleans
      COALESCE(has_qv,       FALSE) AS has_qv,
      COALESCE(has_pdp,      FALSE) AS has_pdp,
      COALESCE(has_atc,      FALSE) AS has_atc,
      COALESCE(has_purchase, FALSE) AS has_purchase,

      -- times from staging
      event_ts,        -- derived from raw `timestamp`
      collector_ts     -- derived from raw `collector_tstamp`
  FROM {{ ref('stg_user_journey_yl') }}
  {% if is_incremental() %}
    -- 2-day sliding window on collection time (captures late arrivals)
    WHERE DATE(collector_ts) >= CURRENT_DATE() - 2
  {% endif %}
),

-- enforce FK validity (avoid orphans)
validated AS (
  SELECT e.*
  FROM engagement_base e
  INNER JOIN {{ ref('dim_user_yl') }}    u ON u.user_id    = e.user_id
  INNER JOIN {{ ref('dim_product_yl') }} p ON p.product_id = e.product_id
)

SELECT
    journey_event_sk,
    user_id,
    product_id,
    search_event_id,
    session_id,
    cart_id,

    event_ts,
    TO_DATE(event_ts) AS event_date,
    collector_ts,

    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
   '{{ run_started_at }}'::timestamp_tz AS dbt_processed_at
FROM validated




















