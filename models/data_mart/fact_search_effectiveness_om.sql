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
   FROM {{ source('de_project', 'user_journey') }} uj
),


valid_users AS (
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