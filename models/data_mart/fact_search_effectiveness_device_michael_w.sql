{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey_device AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.timestamp,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.session_id,
       uj.device_class, 
       CAST(uj.br_viewwidth AS INTEGER) AS br_viewwidth,
       CAST(uj.br_viewheight AS INTEGER) AS br_viewheight, 
       uj.dvce_screenwidth, 
       uj.dvce_screenheight, 
       CAST(uj.doc_width AS INTEGER) AS doc_width,
       CAST(uj.doc_height AS INTEGER) AS doc_height,
   FROM {{ source('de_project', 'user_journey') }} uj
)

SELECT * FROM user_journey_device