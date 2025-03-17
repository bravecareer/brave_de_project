{{ config(
   materialized='incremental',
   unique_key=['user_id', 'search_event_id', 'product_id', 'timestamp']
) }}

WITH user_journey AS (
   SELECT
       uj.user_id,
       uj.product_id,
       -- Search, session and cart details
       uj.search_event_id,
       uj.session_id,
       uj.cart_id,
       uj.search_terms,
       uj.search_results_count AS search_results, -- Renaming column for clarity
       uj.search_type,
       -- User interaction flags
       uj.has_qv,
       uj.has_pdp,
       uj.has_atc,
       uj.has_purchase,
       -- Marketing info
       uj.mkt_campaign,
       uj.mkt_content,
       uj.mkt_medium,
       uj.mkt_source,
       -- Order info
       uj.banner, -- Brand of store
       CAST(uj.selected_store_id AS int) AS selected_store_id,
       uj.fulfillment_type,
       -- Platform info
       uj.app_id,
       uj.device_class,
       -- Geographic data
       uj.geo_city,
       uj.geo_country,
       uj.geo_latitude,
       uj.geo_longitude,
       uj.geo_region,
       uj.geo_timezone,
       uj.geo_zipcode,
       -- Timestamp
       uj.timestamp
   FROM {{ source('de_project', 'user_journey') }} uj
)

SELECT * FROM user_journey
{% if is_incremental() %}
-- On incremental runs, only process new transactions, up to two days ago
WHERE timestamp >= (SELECT DATEADD(day, -2, max(timestamp)) from {{ this }})
{% endif %}