{{ config(
   materialized='incremental',
   unique_key='search_event_id'
) }}

WITH mkt_data AS(
    SELECT
        m.search_event_id,
        m.user_id,
        m.product_id,
        m.timestamp,
        m.mkt_campaign,
        m.mkt_source,
        m.mkt_medium,
        m.mkt_content,
        m.geo_country,
        m.geo_region,
        m.geo_city,
        m.geo_zipcode,
        m.geo_latitude,
        m.geo_longitude,
        m.geo_timezone,
        m.has_purchase,
        m.has_pdp,
        m.has_atc
    FROM
        {{ ref('stg_userjourney_sp') }} m
)

SELECT * FROM mkt_data