{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       dp.product_name,  -- Added product_name from dim_product_data_gs
       uj.has_purchase,
       uj.search_event_id,
       uj.session_id,
       uj.cart_id,
       uj.timestamp,
       uj.updated_at  -- Keeping updated_at from source
   FROM {{ ref('view_user_journey_transformed_gs') }} uj
   LEFT JOIN {{ ref('dim_product_data_gs') }} dp
   ON uj.product_id = dp.product_id
   WHERE uj.has_purchase = TRUE
   {% if is_incremental() %}
   AND uj.updated_at > COALESCE(
       (SELECT MAX(updated_at) FROM {{ this }}), 
       '1990-01-01'
   )
   {% endif %}
)

SELECT * FROM user_journey

{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
