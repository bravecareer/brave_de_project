{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.timestamp,
       uj.search_event_id,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.updated_at  -- Keep updated_at from the transformed table
   FROM {{ ref('view_user_journey_transformed_gs') }} uj
   {% if is_incremental() %}
   WHERE uj.updated_at > COALESCE(
       (SELECT MAX(updated_at) FROM {{ this }}), 
       '1990-01-01'
   )
   {% endif %}
)

SELECT 
    uj.user_id,
    uj.product_id,
    uj.search_event_id,
    uj.has_qv,
    uj.has_pdp,
    uj.has_atc,
    uj.has_purchase,
    uj.updated_at  -- Use existing updated_at instead of CURRENT_TIMESTAMP
FROM user_journey as uj
