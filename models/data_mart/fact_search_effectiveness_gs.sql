{{ config(
   materialized='incremental',
   unique_key=['search_event_id', 'user_id', 'timestamp']
) }}

WITH search_effectiveness AS (
   SELECT
       uj.search_event_id,
       uj.user_id,
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
       uj.updated_at
   FROM {{ ref('user_journey_transformed_gs') }} uj
   WHERE uj.search_event_id IS NOT NULL
   {% if is_incremental() %}
   AND uj.updated_at > COALESCE(
       (SELECT MAX(updated_at) FROM {{ this }}), 
       '1990-01-01'
   )
   {% endif %}
)

SELECT 
    se.search_event_id,
    se.user_id,
    se.search_terms,
    se.search_results_count,
    se.has_qv,
    se.has_pdp,
    se.has_atc,
    se.has_purchase,
    se.search_type,
    se.search_model,
    se.session_id,
    se.timestamp,
    CURRENT_TIMESTAMP AS updated_at
FROM search_effectiveness se