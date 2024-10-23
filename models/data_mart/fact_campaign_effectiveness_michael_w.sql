{{ config(
   materialized='incremental',
   unique_key= ['search_event_id', 'product_id', 'timestamp']
) }}


WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.timestamp,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.mkt_medium,
       uj.mkt_source,
       uj.mkt_content,
       uj.mkt_campaign
   FROM {{ source('de_project', 'user_journey') }} uj
)

SELECT * FROM user_journey