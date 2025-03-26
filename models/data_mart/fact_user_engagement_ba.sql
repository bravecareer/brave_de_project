{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'timestamp']
) }}


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
       to_timestamp(replace(timestamp,' UTC','')) AS timestamp
   FROM {{ ref('stg_user_journey_ba') }} uj
   WHERE timestamp >= CURRENT_DATE() - 5

),


valid_users AS (
   SELECT
       u.user_id
   FROM {{ ref('stg_user_data_ba') }} u
   WHERE u.account_status = 'active'
),


valid_products AS (
   SELECT
       p.product_id
   FROM {{ ref('stg_product_data_ba') }} p
),


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
   FROM {{ ref('stg_user_journey_ba') }} uj
   LEFT JOIN valid_users vu ON uj.user_id = vu.user_id
   LEFT JOIN valid_products vp ON uj.product_id = vp.product_id
)


SELECT * FROM final