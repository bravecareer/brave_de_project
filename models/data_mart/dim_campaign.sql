{{ config(
   materialized='table',
   unique_key='campaign_id'
) }}

SELECT DISTINCT
   mkt_campaign AS campaign_id,
   mkt_medium,
   mkt_source,
   mkt_content
FROM {{ ref('stg_user_journey') }}
WHERE mkt_campaign IS NOT NULL
