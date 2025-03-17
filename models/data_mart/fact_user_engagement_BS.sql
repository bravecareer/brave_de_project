{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'timestamp']
) }}


WITH valid_users AS (
   SELECT
       u.user_id
   FROM {{ source('de_project', 'user_data') }} u
   WHERE u.account_status = 'active'
),

valid_products AS (
   SELECT
       p.product_id
   FROM {{ source('de_project', 'product_data') }} p
),

user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.session_id,
       to_timestamp(replace(timestamp, ' UTC', '')) AS timestamp
   FROM {{ source('de_project', 'user_journey') }} uj
   WHERE timestamp >= DATEADD(MONTH, -12, CURRENT_DATE())
     AND uj.user_id IN (SELECT user_id FROM valid_users)  -- Filter early
     AND uj.product_id IN (SELECT product_id FROM valid_products)  -- Filter early
)

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

WHERE
{% if is_incremental() %}
    timestamp > (select max(timestamp) from {{ this }})
{% else %}
TRUE
{% endif %}