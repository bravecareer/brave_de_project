{{ config(
   materialized='incremental',
   unique_key=['search_event_id', 'user_id', 'product_id', 'timestamp']
) }}

WITH search_effectiveness AS (
   SELECT
       uj.search_event_id,
       uj.user_id,
       uj.product_id,  -- Added product_id
       uj.search_terms,
       uj.search_results_count,
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.search_type,
       uj.search_model,
       uj.session_id,
       uj.timestamp,
       uj.updated_at  -- Keeping updated_at from source
   FROM {{ ref('user_journey_transformed_gs') }} uj
   WHERE uj.search_event_id IS NOT NULL
   {% if is_incremental() %}
   AND uj.updated_at > COALESCE(
       (SELECT MAX(updated_at) FROM {{ this }}), 
       '1990-01-01'
   )
   {% endif %}
)

SELECT * FROM search_effectiveness

{% if is_incremental() %}
WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
