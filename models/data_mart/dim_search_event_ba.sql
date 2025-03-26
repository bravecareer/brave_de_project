{{ config(
   materialized='table',
   unique_key='search_event_id'
) }}


WITH search_event_data AS (
   SELECT
       se.search_event_id,
       se.session_id,
       se.cart_id,
       se.search_terms as search_items,
       se.search_results_count AS search_results, -- Renaming column for clarity
       se.search_type,
       se.search_request_id,
       se.search_feature,
       se.search_terms_type as terms_type,
       se.shopping_mode,
       se.device_class,
       se.timestamp
   FROM {{ ref('stg_user_journey_ba') }} se
   --WHERE se.search_event_id IS NOT NULL
      --AND timestamp >= CURRENT_DATE() - 5
)


SELECT * FROM search_event_data