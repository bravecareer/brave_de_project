{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}


WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.timestamp,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.search_terms_type, 
       uj.login_status, 
       uj.registration_status, 
       uj.banner, 
       lower(substr(uj.br_lang, 1, 2)) AS br_lang, 
       uj.page_language,
       uj.search_model
   FROM {{ source('de_project', 'user_journey') }} uj
)

SELECT * FROM user_journey