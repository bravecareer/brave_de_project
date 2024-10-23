{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}


WITH search_event_data AS (
   SELECT
       se.search_event_id,
       se.timestamp,
       se.app_id,
       se.has_pdp,
       se.has_atc,
       se.has_purchase,
       se.impressions_with_attributions,
       se.search_results_count AS search_results, -- Renaming column for clarity
       se.search_terms,
       se.search_feature,
       se.search_terms_type,
       se.search_type,
       se.login_status,
       se.user_id,
       se.registration_status,
       se.banner,
       se.fulfillment_type,
       CAST(se.selected_store_id AS INTEGER) AS selected_store_id,
       se.selected_timeslot_date,
       se.selected_timeslot_time,
       se.device_class,
       CAST(se.br_viewwidth AS INTEGER) AS br_viewwidth,
       CAST(se.br_viewheight AS INTEGER) AS br_viewheight,
       se.dvce_screenwidth,
       se.dvce_screenheight,
       CAST(se.doc_width AS INTEGER) AS doc_width,
       CAST(se.doc_height AS INTEGER) AS doc_height,
       se.mkt_medium,
       se.mkt_source,
       se.mkt_content,
       se.mkt_campaign,
       se.page_language,
       se.geo_country,
       se.geo_region,
       se.geo_city,
       se.geo_zipcode,
       se.geo_latitude,
       se.geo_longitude,
       se.geo_timezone,
       se.search_model,
       se.product_id
   FROM {{ source('de_project', 'user_journey') }} se
   WHERE se.search_event_id IS NOT NULL
)

SELECT * FROM search_event_data