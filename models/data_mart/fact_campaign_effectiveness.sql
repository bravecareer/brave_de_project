{{ config(
   materialized='incremental',
   unique_key=['campaign_id', 'session_id']
) }}

SELECT
   mkt_campaign AS campaign_id,
   session_id,
   COUNT(has_purchase) AS total_purchases
FROM {{ ref('stg_user_journey') }}
GROUP BY campaign_id, session_id
