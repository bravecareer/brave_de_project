{{ config(
   materialized='incremental',
   unique_key= ['search_event_id']
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
   FROM {{ ref('stg_userjourney_sp') }} uj
),


valid_users AS (
   SELECT
       u.user_id
   FROM {{ ref('stg_userdata_sp') }} u
   WHERE u.account_status = 'active'
),


valid_products AS (
   SELECT
       p.product_id
   FROM {{ ref('stg_products_sp') }} p
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
   FROM user_journey uj
   INNER JOIN valid_users vu ON uj.user_id = vu.user_id
   INNER JOIN valid_products vp ON uj.product_id = vp.product_id
)


SELECT * FROM final