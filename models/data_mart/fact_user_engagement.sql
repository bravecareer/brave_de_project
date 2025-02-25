{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'timestamp']
) }}

-- Get user journey data with basic filtering
WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.session_id,
       uj.event_timestamp as timestamp
   FROM {{ ref('stg_user_journey') }} uj
   WHERE event_timestamp >= CURRENT_DATE() - 5
),

-- Filter for active users only
valid_users AS (
   SELECT
       u.user_id
   FROM {{ ref('stg_user_data') }} u
   WHERE u.account_status = 'active'
),

-- Get valid products
valid_products AS (
   SELECT
       p.product_id
   FROM {{ ref('stg_product_data') }} p
),

-- Combine all data with validations
final AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.session_id,
       uj.timestamp
   FROM user_journey uj
   LEFT JOIN valid_users vu ON uj.user_id = vu.user_id
   LEFT JOIN valid_products vp ON uj.product_id = vp.product_id
)

SELECT * FROM final