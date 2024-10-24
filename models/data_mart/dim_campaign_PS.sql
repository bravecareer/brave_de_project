-- Dimension Table: dim_campaign_PS

{{ config(materialized='table', unique_key='campaign_id') }}

WITH campaign_context AS (
  SELECT DISTINCT
    uj.MKT_CAMPAIGN AS campaign_id,
    uj.BANNER AS banner,
    uj.MKT_MEDIUM AS mkt_medium,
    uj.MKT_SOURCE AS mkt_source,
    uj.MKT_CONTENT AS mkt_content,
    uj.SEARCH_EVENT_ID AS search_event_id,
    uj.SESSION_ID AS session_id,
    uj.GEO_COUNTRY AS geo_country,
    uj.GEO_REGION AS geo_region,
    uj.GEO_CITY AS geo_city,
    uj.GEO_ZIPCODE AS geo_zipcode,
    uj.GEO_LATITUDE AS geo_latitude,
    uj.GEO_LONGITUDE AS geo_longitude,
    uj.GEO_TIMEZONE AS geo_timezone,
    uj.SHOPPING_MODE AS shopping_mode,
    uj.DEVICE_CLASS AS device_class,
    uj.PAGE_LANGUAGE AS page_language,
    uj.BR_LANG AS br_lang
  FROM {{ ref('stg_user_journey_PS') }} uj
  WHERE uj.MKT_CAMPAIGN IS NOT NULL
)

SELECT *
FROM campaign_context
