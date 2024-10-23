{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}

with source as (

   select * from  {{ ref ('stg_user_journey_qu') }}

),

search_event_data AS (
   SELECT
      
      search_model,
      search_terms,
      search_results_count,
      search_type,
      search_feature,
      search_terms_type,
      search_timestamp,
      shopping_mode,
      country,
      city,
      search_event_id,
      session_id,
      user_id,
      product_id
   FROM source
)


SELECT * FROM search_event_data