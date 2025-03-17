{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}


WITH search_effectiveness AS (
   SELECT
       ue.product_id
        ue.has_qv,
       ue.has_pdp,
       ue.has_atc,
       ue.has_purchase,
       se.search_event_id,
       se.session_id,
       se.journey_id,
       se.cart_id,
       se.search_terms,
       se.search_results_count AS search_results, -- Renaming column for clarity
       se.search_type,
       se.timestamp
   FROM {{ ref('fact_user_engangement_bl') }} ue
   LEFT JOIN {{ ref('dim_search_event_bl') }} se
   GROUP BY ue.product_id
)


SELECT * FROM search_effectiveness