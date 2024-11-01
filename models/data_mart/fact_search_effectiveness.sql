{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}

SELECT
   search_event_id,
   product_id,
   COUNT(has_atc) AS add_to_cart_count
FROM {{ ref('stg_user_journey') }}
GROUP BY search_event_id, product_id
