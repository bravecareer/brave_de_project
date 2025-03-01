{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.session_id,
       uj.timestamp,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       CURRENT_TIMESTAMP AS updated_at
   FROM {{ ref('user_journey_transformed_gs') }} uj
   {% if is_incremental() %}
   WHERE uj.updated_at > COALESCE(
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
    uj.timestamp,
    uj.has_qv,
    uj.has_pdp,
    uj.has_atc,
    uj.has_purchase,
    CURRENT_TIMESTAMP AS updated_at
FROM user_journey as uj
