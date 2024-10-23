{{ config(
   materialized='incremental',
   unique_key= ['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey_geo AS (
   SELECT
       uj.user_id,
       uj.product_id,
       uj.search_event_id,
       uj.timestamp,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       uj.geo_country, 
       uj.geo_region, 
       uj.geo_city, 
       uj.geo_zipcode, 
       uj.geo_latitude, 
       uj.geo_longitude, 
       uj.geo_timezone
   FROM {{ source('de_project', 'user_journey') }} uj
)

SELECT * FROM user_journey_geo