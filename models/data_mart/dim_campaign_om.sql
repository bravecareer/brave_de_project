{{ config(
   materialized='incremental',
   unique_key='campaign_id'
) }}

WITH campaign_data AS (
   SELECT
       mkt_campaign AS campaign_id,
       mkt_source AS campaign_source,
       mkt_medium AS campaign_medium,
       mkt_content AS campaign_content,
       geo_country AS campaign_geo_country,
       geo_city AS campaign_geo_city
   FROM {{ source('de_project', 'user_journey') }} -- Assuming marketing data is within user journey data
   WHERE mkt_campaign IS NOT NULL
)

SELECT * FROM campaign_data
