{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}

WITH journey_cleaned AS (
   SELECT
       search_event_id,
       timestamp,
       app_id,
       has_qv,
       has_pdp,
       has_atc,
       has_purchase,
       cart_id,
       session_id,
       search_request_id,
       search_results_count,
       search_terms,
       search_feature,
       search_terms_type,
       search_type,
       date_last_login,
       date_last_purchase,
       user_id,
       registration_status,
       banner,
       mkt_medium,
       mkt_source,
       mkt_content,
       mkt_campaign,
       geo_country,
       geo_region,
       geo_city,
       geo_zipcode,
       geo_latitude,
       geo_longitude,
       geo_timezone,
       product_id
   FROM {{ source('de_project', 'user_journey') }}
   WHERE search_event_id IS NOT NULL
)

SELECT * FROM journey_cleaned
{% if is_incremental() %}
    WHERE search_event_id NOT IN (SELECT search_event_id FROM {{ this }})
{% endif %}
