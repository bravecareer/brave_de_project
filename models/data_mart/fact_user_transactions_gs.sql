{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.has_purchase,
       uj.search_event_id,
       uj.session_id,
       uj.cart_id,
       uj.timestamp,
       uj.updated_at
   FROM {{ ref('user_journey_transformed_gs') }} uj
   WHERE uj.has_purchase = TRUE
   {% if is_incremental() %}
   AND uj.updated_at > COALESCE(
       (SELECT MAX(updated_at) FROM {{ this }}), 
       '1990-01-01'::TIMESTAMP_NTZ
   )
   {% endif %}
)

SELECT 
    uj.user_id,
    uj.product_id,
    uj.search_event_id,
    uj.session_id,
    uj.cart_id,
    uj.timestamp,
    uj.has_purchase,
    CURRENT_TIMESTAMP AS updated_at
FROM user_journey as uj
