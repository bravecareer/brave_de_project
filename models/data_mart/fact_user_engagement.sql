-- fact_user_engagement.sql: Incremental fact table for user engagement events
{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'timestamp']
) }}

-- Filter user journey data for recent events (last 5 days) and parse timestamps
WITH recent_user_journey AS (
   SELECT
       uj.user_id,                      -- User identifier
       uj.product_id,                   -- Product involved in user journey
       uj.search_event_id,              -- Search event associated with journey
       uj.has_qv,                       -- Quick view interaction flag
       uj.has_pdp,                      -- Product detail page interaction flag
       uj.has_atc,                      -- Add to cart interaction flag
       uj.has_purchase,                 -- Purchase action flag
       uj.session_id,                   -- Session identifier
       to_timestamp(replace(uj.timestamp, ' UTC', '')) AS timestamp -- Converted timestamp
   FROM {{ source('de_project', 'user_journey') }} uj
   WHERE to_date(replace(uj.timestamp, ' UTC', '')) >= CURRENT_DATE() - 5
),

-- Identify valid active users
valid_users AS (
   SELECT u.user_id
   FROM {{ source('de_project', 'user_data') }} u
   WHERE u.account_status = 'active'
),

-- Identify valid products
valid_products AS (
   SELECT p.product_id
   FROM {{ source('de_project', 'product_data') }} p
),

-- Join user journey data with valid users and products
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
   FROM recent_user_journey uj
   INNER JOIN valid_users vu ON uj.user_id = vu.user_id         -- Keep only active users
   INNER JOIN valid_products vp ON uj.product_id = vp.product_id -- Keep only valid products
)

-- Explicit column selection for clarity
SELECT
    user_id,
    product_id,
    search_event_id,
    has_qv,
    has_pdp,
    has_atc,
    has_purchase,
    session_id,
    timestamp
FROM final
